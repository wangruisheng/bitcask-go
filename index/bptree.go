package index

import (
	"go.etcd.io/bbolt"
	"myRosedb/data"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// B+ 树索引
// 主要封装了 go.etcd.io/bbolt 库
type BPlusTree struct {
	tree *bbolt.DB // bbolt 其实也是数据库
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	// 是否每次持久化
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	// 因为bbolt支持多线程，所以不用加锁
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		// 每次update都相当于提交一个事务(自动提交)，tx就是事务
		// bucket相当于一个数据的分区
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	// 取出对应的 bucket，用bucket读写数据
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		// key 和 value 都只能是 byte数组，所以要对pos进行编码
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	// view方法只能读数据
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		// ??? 这里没有返回错误啊，报错从哪里返回
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool = false
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		// key 和 value 都只能是 byte数组，所以要对pos进行编码
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	// view方法只能读数据
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+ 树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	// 手动开启事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failee to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	// 调用的时候就已经返回了 kv，所以要先将 kv 暂存
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}

}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
