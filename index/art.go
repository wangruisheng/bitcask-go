package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"myRosedb/data"
	"sort"
	"sync"
)

// 自适应基数树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 初始化自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, _ := art.tree.Insert(key, pos)
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, deleted := art.tree.Delete(key)
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	size := art.tree.Size()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.Lock()
	defer art.lock.Unlock()
	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Art 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key + 位置索引信息
}

// ?? 为什么btree这里要传指针，art则不用
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		// 这个不用排序吗，还是说art树本来就是递增的？
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	// forEach方法为每个叶子节点执行一次指定的回调函数
	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (ai *artIterator) Rewind() {
	ai.currIndex = 0

}

// Seek 根据传入的 Key 查找到第一个大于（或小于）等于的目标 key，从这个 key 开始遍历
// 和 for 结合使用
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		// 因为 values 已经被排好序了，所以可以用二分查找进行查找
		// 我的理解，compare就是让谁比谁，i就是从[0,len(bti.values))依次遍历
		// Search 会返回找到的下标i
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (ai *artIterator) Next() {
	ai.currIndex += 1
}

// Valid 是否有效，即是否已经完成遍历完所有的key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的 key
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	ai.values = nil
}
