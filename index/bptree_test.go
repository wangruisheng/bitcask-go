package index

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/data"
	"os"
	"path/filepath"
	"testing"
)

func TestNewBPlusTree(t *testing.T) {
	// E:\tmp
	path := filepath.Join("/tmp")
	// ???用了但是没删除呀
	defer func() {
		err := os.RemoveAll(path)
		t.Log(err)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res3)

	res4 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 7744, Offset: 883})
	assert.Equal(t, uint32(123), res4.Fid)
	assert.Equal(t, int64(999), res4.Offset)
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	t.Log(pos)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("key1"))
	t.Log(pos1)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 333, Offset: 8888})
	pos2 := tree.Get([]byte("key1"))
	t.Log(pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join("/tmp")
	// pathFile := filepath.Join("/tmp", bptreeIndexFileName)
	defer func() {
		// 为什么只删除文件夹，不删除文件
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	// 之所以能拿到，是因为之前的目录没有删除
	res1, ok1 := tree.Delete([]byte("not exist"))
	assert.False(t, ok1)
	assert.Nil(t, res1)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("key1"))
	t.Log(pos1)
	res2, ok2 := tree.Delete([]byte("key1"))
	assert.True(t, ok2)
	assert.NotNil(t, res2)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 333, Offset: 8888})
	pos2 := tree.Get([]byte("key1"))
	t.Log(pos2)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join("/tmp")

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	//
	size := tree.Size()
	t.Log(size)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 123, Offset: 999})
	size = tree.Size()
	t.Log(size)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join("/tmp")
	// ???用了但是没删除呀
	defer func() {
		err := os.RemoveAll(path)
		t.Log(err)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}

}
