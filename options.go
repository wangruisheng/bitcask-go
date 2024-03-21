package bitcask_go

import "os"

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	// 这里直接用index包的不行吗，为什么还要再定义一遍
	IndexType IndexerType

	// 启动时是否使用 MMap 进行加载
	MMapAtStartup bool

	// 数据文件合并的阈值，无效文件在总数量当中的比例
	DataFileMergeRatio float32
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定的 Key，默认为空
	Prefix []byte

	// 是否是反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchOption 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 在提交数据的时候是否进行 Sync 持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota

	// ART Adaptive Radix Tree 自适应基数树索引
	ART

	// BPlusTree B+ 树索引，将索引储存到磁盘上
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0, // 默认不开启
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
