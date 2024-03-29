package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"myRosedb/fio"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// 创建 数据文件 结构体
type DataFile struct {
	FileID    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // IO 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//具体来说，%09d中的%d是用于表示整数的占位符，而09表示将整数格式化为9位宽度，并在左侧用零进行填充（如果需要的话）。
	//例如，假设有一个整数值为123，则使用%09d格式化后的结果为"000000123"，宽度为9位，不足的位数用零进行填充。
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 打开储存事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// 拿到数据文件的名字
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// return filepath.Join(dirPath, fmt.Sprintf("#{fileId}", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	// 每次打开都是0吗，不应该看已经写入了多少，作为writeOff吗
	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// 这里其实可以优化，因为wirte进去的时候是数组，read却得到了logRecord。意味着datafile负责了解码的功能，所以最好是都让logrecord负责
// 返回读了多少
// ReadLogRecord 根据 offset 从数据文件中读取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 头部都是变长的，那怎么知道头部长度是多少
	// 先读最大的值，返回回来的byte[]里一定包含着应该有的头部信息
	// 再用decodeLogRecordHeader()方法对该byte数组进行解析，得到实际的头部长度以及value长度
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// 读出来的头是编码过得，我们要进行解码
	// 得到实际的header
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// ??? 这里最开始怎么忘加了，导致文件已经读到最后，返回的header=nil时一直报错
	if header == nil {
		return nil, 0, io.EOF
	}
	// if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
	//		return nil, 0, io.EOF
	//	}

	// 定义 LogRecord 结构体
	logRecord := &LogRecord{Type: header.recordType}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	// 开始读取用户实际存储的 key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解出 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性，用crc校验
	// crc 前面 4 个字节不用进行校验
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	// 与存储在数据文件中的crc进行比较
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key: key,
		// value 就是位置索引信息
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// 持久化 数据文件
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// 关闭文件
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileID), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

// 直接在返回值中定义变量名，就可以直接赋值了
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
