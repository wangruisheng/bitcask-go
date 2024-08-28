package index

import (
	"math/rand"
	"time"
)

const (
	MaxLevel    int     = 32   // 最大的 level 数
	Probability float64 = 0.25 // 生成下一个节点的概率是0.25
	GE          int     = 0    // greater than or equal to
	LE          int     = 1    // less than or equal to 小于等于
)

type SkipList struct {
	len   int64         // 跳表的长度，以便在O(1)的时间复杂度获得节点的数量
	head  *skipListNode // 跳表的头尾节点，便于在O(1)时间复杂度内访问跳表的头节点和尾节点【头节点不存储数据】
	tail  *skipListNode
	level int // 跳表的层数，便于在O(1)时间复杂度获取跳表中层高最大的那个节点的层数量【当前跳表的最高层数】
}

type skipListNode struct {
	member string
	score  float64
	next   []*skipListNode // 其实不用保存跨度，下标i+1就代表跨度
	// 【未必代表跨度，因为可以两个两层的节点在一起，则第一个两层节点的跨度都是1】
	backward *skipListNode
}

//type skipListlevel struct {
//	span    int64 // 保存当前层到下一个节点的跨度
//	forward *skipListNode
//}

func NewSkipList() *SkipList {
	head := NewSkipListNode("", 0, MaxLevel)
	return &SkipList{
		len:   0,
		head:  head, // 创建头节点会创建最大层数的节点
		tail:  head,
		level: 1,
	}
}

func NewSkipListNode(member string, score float64, level int) *skipListNode {
	return &skipListNode{
		member: member,
		score:  score,
		next:   make([]*skipListNode, level),
	}
}

// 寻找节点
// opt == 0，找到第一个 >= score 的节点
// opt == 1，找到最后一个 <= score 的节点
func (sl *SkipList) Find(score float64, opt int) *skipListNode {
	x := sl.head

	// 查找节点
	if opt == GE {
		for i := sl.level - 1; i >= 0; i-- {
			// 找到目标节点的上一个节点
			// 不用做score相等，按member找的判断。因为满足第一个 >= score 的节点即可
			// 不能是 x.next[i].score <= score，不然就会找到满足最后一个 >= score 的节点
			for x.next[i] != nil && (x.next[i].score < score) {
				x = x.next[i]
			}
			x = x.next[i]
			if x != nil && x.score >= score {
				return x
			}
		}
	} else {
		for i := sl.level - 1; i >= 0; i-- {
			// 如果有 0 1 2 2 2 3，score为2
			// 会找到：       ——
			for x.next[i] != nil && (x.next[i].score <= score) {
				x = x.next[i]
			}
			if x != nil && x.score <= score {
			}
			return x
		}
	}
	return nil
}

func (sl *SkipList) Insert(score float64, member string) *skipListNode {

	// 将每一层的前置节点存放在update当中
	update := make([]*skipListNode, MaxLevel)
	x := sl.head
	// 从最顶层开始找目标位置每一层的前一个节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && (x.next[i].score < score ||
			(x.next[i].score == score && x.next[i].member < member)) {
			x = x.next[i] // 接着往下找
		}
		// 找到要插入的前一个节点
		update[i] = x
	}

	// 插入新节点
	// 随机初始化层高
	// 查看新节点的层数 lvl 是否大于当前跳表的最大层数
	lvl := sl.randomLevel()
	if lvl > sl.level {
		// 如果新节点的层数大于跳表当前的，则将多出的层的上一个指向头节点
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	// 其他节点的处理
	newNode := NewSkipListNode(member, score, lvl)
	for i := 0; i < lvl; i++ {
		if i == 0 {
			newNode.backward = update[i]
		}
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.len++

	return newNode
}

func (sl *SkipList) Delete(score float64, member string) {

	// 存储要删除的上一个节点
	update := make([]*skipListNode, MaxLevel)
	x := sl.head
	// 遍历顺序一定得从level最高层开始，因为层数越高跨度越大，如果先走最底层，x就会往后走，高层的前一个x就无法存储到
	// 这就是跳表查找速度快的地方，不想链表要逐个遍历，跳表可以直接跳到接近目标节点的地方
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && (x.next[i].score < score ||
			(x.next[i].score == score && x.next[i].member < member)) {
			x = x.next[i]
		}
		update[i] = x
	}

	// 查看要删除的节点是否在跳表当中
	x = x.next[0] // x.next[0]就是要删除的节点
	if x != nil && x.score == score && x.member == member {

		// 删除节点
		for i := 0; i < sl.level; i++ {
			// 如果要删除的x高度为2，那么高于2的那些层数就不用更新了
			if update[i].next[i] != x {
				break
			}
			update[i].next[i] = x.next[i]
		}
	}

	// 更新 skipList 的层数
	// 就算跳表删完了，层数最少为1
	// 如果当前层head下一个指向的为nil，层数 - 1
	for sl.level > 1 && sl.head.next[sl.level-1] == nil {
		sl.level--
	}

	sl.len--
}

// 获得一个随机的层高
func (sl *SkipList) randomLevel() int {
	level := 1

	// 对于每一次循环，生成下一层的可能性都会乘以 'Probability'
	for level < MaxLevel && random() < Probability {
		level++
	}
	return level
}

// 产生一个0-1的随机数
func random() float64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Float64()
}
