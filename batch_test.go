package bitcask_go

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/utils"
	"os"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// ？？加上 destroy(db) 则报错panic: remove C:\Users\40466\AppData\Local\Temp\bitcask-go-put2298598267\000000000.data: The process cannot access the file because it is being used by another process.
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据但并不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	value, err := db.Get(utils.GetTestKey(1))
	t.Log("value,err =", value, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	// 把要删除的key追加入数据文件
	err = wb2.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(utils.GetTestKey(1))
	t.Log(val2)
	t.Log(err)

}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// db 已经关闭过了
	// defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)
	// t.Log(db.seqNo)

	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	// t.Log(db.seqNo)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	val, err := db2.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	// db 已经关闭过了
	// defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	for i := 0; i < 500000; i++ {
		wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

}
