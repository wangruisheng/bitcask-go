package index

// zset数据结构的索引
type SortedSet struct {
	record map[string]*zSet
}

// zset数据结构，由 dict 和 skip list 组成
type zSet struct {
	sl *SkipList
	// dict map[string]int
	dict map[string]*skipListNode // 字典，便于以O(1)时间复杂度获得元素的权重
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		record: make(map[string]*zSet),
	}
}

func NewZSet() *zSet {
	return &zSet{
		sl:   NewSkipList(),
		dict: make(map[string]*skipListNode),
	}
}

// 因为不需要知道是否插入了重复数据，所以不用返回true/false
func (ss *SortedSet) Add(key string, member string, score float64) bool {
	if !ss.KeyExist(key) {
		ss.record[key] = NewZSet()
	}

	zset := ss.record[key]
	node, exist := zset.dict[member]

	if exist {
		// 已有则返回false
		if node.score == score {
			return false
		}
		// 如果不相同，则要改变跳表，因为是按score排序，所以得删除原来的member对应的节点
		zset.sl.Delete(score, member)
	}
	Newnode := zset.sl.Insert(score, member)
	zset.dict[member] = Newnode
	return true
}

func (ss *SortedSet) Remove(key, member string) bool {
	if ss.MemberExist(key, member) {
		n := ss.record[key].dict[member]
		// 删除跳表中的节点
		ss.record[key].sl.Delete(n.score, member)
		// 删除字典中的节点
		delete(ss.record[key].dict, member)
		return true
	}
	return false
}

func (ss *SortedSet) KeyExist(key string) bool {
	_, exist := ss.record[key]
	return exist
}

func (ss *SortedSet) MemberExist(key, member string) bool {
	if ss.KeyExist(key) {
		_, ok := ss.record[key].dict[member]
		return ok
	}
	return false
}
