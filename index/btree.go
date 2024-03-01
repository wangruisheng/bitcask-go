package index

import (
	"bytes"
	"github.com/google/btree"
	"myRosedb/data"
	"sort"
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key + 位置索引信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有数据存放到数组中
	// 为什么这样定义函数？？？
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		// 如果返回false会终止tree.Descend()的遍历
		return true
	}
	if reverse {
		// 让树从小到大进行排列
		tree.Descend(saveValues)
	} else {
		// 让树从大到小进行排列
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0

}

// Seek 根据传入的 Key 查找到第一个大于（或小于）等于的目标 key，从这个 key 开始遍历
// 和 for 结合使用
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		// 因为 values 已经被排好序了，所以可以用二分查找进行查找
		// 我的理解，compare就是让谁比谁，i就是从[0,len(bti.values))依次遍历
		// Search 会返回找到的下标i
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效，即是否已经完成遍历完所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 key
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
