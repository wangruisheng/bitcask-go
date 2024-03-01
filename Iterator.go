package bitcask_go

import (
	"bytes"
	"myRosedb/index"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	// 包含索引迭代器，取出 key 和 索引 的信息
	indexIter index.Iterator

	// 面向用户，所以要包含DB
	db *DB

	// 传入用户的索引迭代器配置项
	options IteratorOptions
}

// NewIterator 初始化迭代器，属于DB结构体
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	// 用 db 获取索引
	indexIter := db.index.Iterator(opts.Reverse)

	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的 Key 查找到第一个大于（或小于）等于的目标 key，从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经完成遍历完所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
// 将btreeIterator返回的位置信息进行处理
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		// 如果 prefix长度小于key的长度，并且前缀相等的话，则跳出循环，如果不相等则继续跳转下一个key，进行判断
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
