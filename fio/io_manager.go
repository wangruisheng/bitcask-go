package fio

const DataFilePerm = 0644

// IOManger 抽象 IO 管理接口，可以接入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error
}
