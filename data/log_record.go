package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

// 定义枚举类型
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keySize valueSize
// 4 	+ 1  + 5 	+ 5
// 可变编码是什么意思？
// 头最长可能得值
// 不是可以自动拓展吗，没有分配够长度为什么不会自动扩容，是不是append的时候超出了两倍？
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	// 之前由于没大写，所以外部读取不到
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc校验值
	recordType LogRecordType // 表示 logRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件的哪个位置
}

// 暂存的事务相关的数据
type TranscationRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 对 LogRecord（+ logRecordHeader） 进行编码，返回字节数组以及长度
// +---------------+---------------+---------------+---------------+---------------+---------------+
// |   crc 校验值   |   type 类型    |   key size    |   type size   |   	  key     |   	 type      |
// +---------------+---------------+---------------+---------------+---------------+---------------+
// |     4 字节     |     1 字节     |  变长（最大5）  |  变长（最大5）  |   	  变长     |    	 变长       |
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5

	// 5个字节以后，存储的是 key 和 value 的长度信息
	// 使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// 最终要得到的编码后的 logRecordHeader + logRecord 信息
	encBytes := make([]byte, size)

	// 将 header 部分的内容拷贝过来
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组当中
	// 因为本来就是字节数组，所以不需要进行转化
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 使用go中自带的crc32校验方法
	// 对整个 LogRecord 的数据进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 序列化
	// 调用了 binary中小端序的编码，字节编码一般分为大端序和小端序
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// 测试 header 编码长度使用
	// fmt.Printf("header length = %d, crc = %d\n", index, crc)

	return encBytes, int64(size)
}

// EncodeLogRecordPos 对位置进行编码（用来存入hint文件
//func EncodeLogRecordPos(pos *LogRecordPos) []byte {
//	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
//	var index = 0
//	index += binary.PutVarint(buf[index:], int64(pos.Fid))
//	index += binary.PutVarint(buf[index:], pos.Offset)
//	return buf[:index]
//}
//
//// DecodeLogRecordPos EncodeLogRecordPos编码过后，从 hint 文件中取出时，要进行解码
//func DecodeLogRecordPos(buf []byte) *LogRecordPos {
//	var index = 0
//	fileId, n := binary.Varint(buf[index:])
//	index += n
//	offset, _ := binary.Varint(buf[index:])
//	return &LogRecordPos{Fid: uint32(fileId), Offset: offset}
//}

// 对字节数组中的 Header 信息进行解码
// 此时还没有进行 crc 校验
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// 如果连crc都没有，则报错
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		// 反序列化
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 从第六个字节开始拿 key size 和 value size
	// 取出实际的key size
	// 它怎么知道要反序列化多少个字节？？？
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value Size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 头部都是变长的，那怎么知道头部长度是多少
// header 只是头部的长度
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	// 首先计算 header 部分的crc校验
	// 这个 header 已经是删除了首部crc四个字节的
	crc := crc32.ChecksumIEEE(header[:])
	// 要加上 LogRecord 中的 key 和 value 的值进行crc校验 才是最终crc的值
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
