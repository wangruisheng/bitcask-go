package data

import "myRosedb/fio"

// 创建 数据文件 结构体
type DataFile struct {
	FileID    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // IO 读写管理
}

// 打开 新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// 这里其实可以优化，因为wirte进去的时候是数组，read却得到了logRecord。意味着datafile负责了解码的功能，所以最好是都让logrecord负责
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// 持久化 数据文件
func (df *DataFile) Sync() error {
	return nil
}
