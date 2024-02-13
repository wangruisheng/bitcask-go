package index

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	// 引入断言库
	assert.True(t, res1)

	res2 := bt.Put([]byte{'a'}, &data.LogRecordPos{1, 100})
	// 引入断言库
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// 存两次看是否覆盖
	res2 := bt.Put([]byte{'a'}, &data.LogRecordPos{1, 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte{'a'}, &data.LogRecordPos{1, 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte{'a'})
	// t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put([]byte{'a'}, &data.LogRecordPos{1, 100})
	assert.True(t, res1)
	res2 := bt.Put([]byte{'a'}, &data.LogRecordPos{1, 100})
	assert.True(t, res2)

}
