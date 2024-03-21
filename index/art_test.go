package index

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/data"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)

	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 99, Offset: 88})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(12), res4.Offset)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
	assert.NotNil(t, pos)

	// 试图拿并不存在的数据
	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	// 数据被替换了
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 123, Offset: 123})
	pos2 := art.Get([]byte("key-1"))
	t.Log(pos2)
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	// 删除一个不存在的key
	res1, ok1 := art.Delete([]byte("key-1"))
	assert.Nil(t, res1)
	// t.Log(recordPos, res)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	//t.Log(pos)
	assert.NotNil(t, pos)
	pos2, ok2 := art.Delete([]byte("key-1"))
	assert.NotNil(t, pos2)
	// t.Log(recordPos, res)
	assert.True(t, ok2)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 3, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
