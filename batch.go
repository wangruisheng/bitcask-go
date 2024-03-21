package bitcask_go

import (
	"encoding/binary"
	"myRosedb/data"
	"sync"
	"sync/atomic"
)

// 非事务标记
const nonTransactionSeqNo uint64 = 0

// 定义标识完成事务提交的key
var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.RWMutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 初始化 WriteBach 的方法
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	// 只有当是 B+ 树，并且存储事务序列号不存在，并且不是第一次初始化，都禁用batch
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInital {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.RWMutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在则直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	//暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交的串行化
	// ？？？为什么要用db的锁
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取事务的序列号
	// 这是什么意思 ？？？递增seqNo
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 开始写数据到数据文件当中
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		// 索引等到所有数据写完再更新，所以先暂时将他们暂存起来
		positions[string(record.Key)] = logRecordPos

	}
	// 写一条标识事务完成提交的数据，是保证原子性的关键
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	// 此时所有的数据已经持久化到数据文件当中
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据，方便下一次commit
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// key+Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	// 将 int：seqNo 传入到 buf：seq[:] 当中
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	// 反序列化字节为uint
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
