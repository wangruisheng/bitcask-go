package bitcask_go

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/utils"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
	assert.Equal(t, false, iterator.Valid())
}

// 对数据库中只有一条数据进行测试
func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(20))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
	t.Log(string(iterator.Key()))
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	value, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(20), value)
}

// 数据库中有多条数据的情况
func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("akey1"), utils.RandomValue(10))
	err = db.Put([]byte("bkey2"), utils.RandomValue(10))
	err = db.Put([]byte("ckey3"), utils.RandomValue(10))
	err = db.Put([]byte("dkey4"), utils.RandomValue(10))

	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log("key = ", string(iter1.Key()))
	}
	iter1.Rewind()
	// 使用 seek
	for iter1.Seek([]byte("key2")); iter1.Valid(); iter1.Next() {
		t.Log("key = ", string(iter1.Key()))
	}

	// 反向迭代
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Reverse = true
	iter2 := db.NewIterator(iterOpts2)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
	}
	iter2.Rewind()
	// 使用 seek
	for iter2.Seek([]byte("key2")); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
	}

	// 指定了 prefix
	// 指定a开头的数据，所以只会打印一条
	iterOpts3 := DefaultIteratorOptions
	iterOpts3.Prefix = []byte("a")
	iter3 := db.NewIterator(iterOpts3)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log(string(iter3.Key()))
	}
}
