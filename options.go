package bitcask_go

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 索引类型
	// 这里直接用index包的不行吗，为什么还要再定义一遍
	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota

	// ART Adaptive Radix Tree 自适应基数树索引
	ART
)
