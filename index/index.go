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
	Delete(key []byte) (*data.LogRecordPos, bool)
}

// *btree.BTree.ReplaceOrInsert()方法需要传入Item类型，Item是个接口，需要实现
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {

	// 为什么这里不能用（bi.(Item).key）
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
