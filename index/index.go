package index

import (
	"bytes"
	"github.com/google/btree"
	"myRosedb/data"
)

// Indexer 抽象索引接口，后续如果想要加入其他的数据结构，则直接实现这个接口即可
type Indexer interface {
	// Put 向索引中储存 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据 key 取出对应索引位置的信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应索引位置的信息
	Delete(key []byte) bool
	// Size 索引中的数据量
	Size() int
	// Iterator 返回索引迭代器，用来逐个获取Item（key和pos）（用户看不到Item）
	Iterator(reverse bool) Iterator
	// 关闭索引，因为B+树相当于一个bblot数据库实例，所以如果这里不关闭数据库的话，再打开可能会堵住，因为bblot是单线程的
	Close() error
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota

	// ART 自适应基数索引
	ART

	// BPTree B+ 树索引
	BPTree
)

// NewIndexer 根据类型初始化索引
// 这里返回的 Indexer 类型为什么不是 *Indexer
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

// *btree.BTree.ReplaceOrInsert()方法需要传入Item类型，Item是个接口，需要实现
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 定义 btree 中的比较规则
func (ai *Item) Less(bi btree.Item) bool {

	// 为什么这里不能用（bi.(Item).key）
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 Key 查找到第一个大于（或小于）等于的目标 key，从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经完成遍历完所有的key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 key
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
