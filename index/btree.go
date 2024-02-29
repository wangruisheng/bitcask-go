package index

import (
	"github.com/google/btree"
	"myRosedb/data"
	"sync"
)

// BTree 索引，主要封装了 google 的 btree 库，并实现了 index 接口
// https://github.com/google/btree
// 用btree数据结构实现增删改查，所以要实现index接口
type BTree struct {
	tree *btree.BTree
	// btree库写不安全，读安全，所以要加锁
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		// 初始化 BTree 类型
		tree: btree.New(32),
		// 初始化锁
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	// 为什么这里要加&
	// 有重复的key应该会替换
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key: key}
	btreeItem := bt.tree.Get(&it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(&it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true

}
