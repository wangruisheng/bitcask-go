package index

import (
	"github.com/stretchr/testify/assert"
	"log"
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

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// 1、BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2、BTree 有数据的情况
	bt1.Put([]byte("code1"), &data.LogRecordPos{1, 10})
	// 更新了数据，每次都要重新获取
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 有多条数据
	bt1.Put([]byte("code2"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("code3"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("code4"), &data.LogRecordPos{1, 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		log.Print("key=" + string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		log.Print("key=" + string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// 4、测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("code3")); iter5.Valid(); iter5.Next() {
		log.Print("key=" + string((iter5.Key())))
		assert.NotNil(t, iter5.Key())
	}

	// 4、反向遍历，测试 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("code0")); iter6.Valid(); iter6.Next() {
		log.Print("key=" + string((iter6.Key())))
		assert.NotNil(t, iter6.Key())
	}

}
