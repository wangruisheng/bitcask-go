package bitcask_go

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"myRosedb/data"
	"myRosedb/fio"
	"myRosedb/index"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 这个文件主要存放面相用户的操作接口

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎
type DB struct {
	options         Options
	fileIds         []int
	mu              *sync.RWMutex
	activeFile      *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号，全局递增
	isMerging       bool                      // 是否正在 merge
	seqNoFileExists bool                      // 事务序列号是否存在，用于判断在b+树模式下，是否要禁用掉batch
	isInital        bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              // 文件锁保证多进程之间的互斥
	bytesWrite      uint                      // 累计写了多少个字节
}

// Open 打开存储引擎实例 bitcask
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOption(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 判断数据目录是否存在，如果不存在的话，就创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	// 获取读锁（错误），应该获取写锁（互斥锁），因为读锁多进程之间可以共享
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options: options,
		mu:      new(sync.RWMutex),
		//activeFile: new(data.DataFile),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInital:   isInitial,
		fileLock:   fileLock,
	}
	// 加载 merge 数据目录
	// 有bug，报错
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 重置 IO 类型为标准文件
	if db.options.MMapAtStartup {
		db.resetIoType()
	}
	// 如果索引类型为 b+ 树，则不需要加载索引，因为已经持久化到磁盘中了
	if options.IndexType != BPlusTree {
		// 从 hint 索引文件中加载索引
		// 先查看是否有索引文件
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// 如果索引类型为 B+ 树，则取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("filed to unlock the directory,%v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 写入 Key/Value 数据，Key 不能为空
// db 中的put和delete没有对key和seqNo进行编码，因为他是非事务的
func (db *DB) Put(key []byte, value []byte) error {
	// 先判断 key 是否无效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加数据写入磁盘文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Delete 根据 key 删除对应的数据（直接追加 Type 为 Delete 的logRecord
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在的话直接返回
	// 从索引中拿，索引中的key是不带事务号的
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord，标识是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	// 写入到数据文件当中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中将对应的 key删除
	// 为什么老师的代码只有一个返回值
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果 key 不在内存索引中，如果 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件 id 找到对应的数据文件
	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// ？？？为什么不用 db.NewIterator()
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		// 如果函数返回false，则结束整个遍历
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 将从索引位置获取 value 数据的方法提取出来
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileID == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 按理来说在加载索引的时候，就已经从btree中删除掉了，所以应该找不到改key对应的value。应该不用从这里再判断一次
	if logRecord.Type == data.LogRecordDeleted {
		return nil, err
	}

	return logRecord.Value, nil

}

// 因为在batch中Commit()调用了appenLogRecord方法，但Commit()已经加锁了，所以需要一个不加锁的appendLogRecord，就单独将加锁的方法提取出来
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 定义 LogRecord 写入磁盘方法，方法不用大写，因为是内部方法
// 返回内存索引信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 对数据文件进行操作
	// 对写入数据 logRecord 进行编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到活跃文件的1阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 正式进行写入操作
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	// 看用户是否每次进行写入后都想要进行持久化，根据用户配置决定
	var needSync = db.options.SyncWrites
	// 如果没有打开每次持久化，并且写入字节数持久化>0
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构建内存索引信息并返回
	logRecordPos := &data.LogRecordPos{db.activeFile.FileID, writeOff}
	return logRecordPos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		// 新的活跃文件id 在上一个之上 1
		initialFileID = db.activeFile.FileID + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	// 读取目录中的所有条目
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 000001.data，将文件id解析
			splitNames := strings.Split(entry.Name(), ".")
			fmt.Println(splitNames, splitNames[0])
			// 包strconv实现了与基本数据类型的字符串表示形式之间的转换，Atoi相当于ParseInt（s,10,0），转换为int类型。
			// 这里乱码了，原因是写文件名的时候代码有错误
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIds = append(fileIds, fileId)

		}
	}

	// 对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件 id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		// 把最新的（id最大的）文件设置为活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge（如果比nonMergeFileId大的话才要加载索引）
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileID(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// 新定一个更新内存索引的方法，因为要重复使用
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	// uint64 是事务的id，如果判断到事务的id可以提交了，就将事务取出来，更新内存索引
	transcationRecords := make(map[uint64][]*data.TranscationRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// EOF is the error returned by Read when no more input is available. 没有更多可读的了，就跳出本次循环
				// ??? 这里写错了，应该是==，原来写成了!=
				if err == io.EOF {
					break
				} else {
					return err
				}
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{fileId, offset}

			// 从数据文件中加载索引的时候，要读到最后一位提交完成标识再更新入索引
			// 解析 key，拿到事务序列号（因为key是经过 key+seqNo编码的）
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transcationRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transcationRecords, seqNo)
				} else {
					// batch当中写入的数据，但是还没有判断是否提交成功，则先暂存起来
					logRecord.Key = realKey
					transcationRecords[seqNo] = append(transcationRecords[seqNo], &data.TranscationRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			// 更新事务序列号
			// 保证db拿到最新的序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//if logRecord.Type == data.LogRecordDeleted {
			//	// 都没加为什么要删？
			//	// 因为在重启数据库的时候，如果我们不对已删除的数据进行处理的话，内存索引是不会知道的，那么被删除的数据对应的索引仍然存在，会导致已经被删除的数据又存在了，数据会发生不一致
			//	// 所以在启动 bitcask 实例，从数据文件加载索引的时候，需要对已删除的记录进行处理（因为数据文件中，同一个key的被删除记录总在加入记录之后，所以查找到删除type的时候，这个	key 之前肯定被加进来过）
			//	// 如果判断到当前处理的记录是已删除的，则根据对应的key将内存索引中的数据删除
			//	db.index.Delete(logRecord.Key)
			//} else {
			//	db.index.Put(logRecord.Key, logRecordPos)
			//}

			// 递增 offset， 下一次从新的位置开始
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	// 更新事务序列号
	db.seqNo = currentSeqNo

	return nil

}

// 为什么定义函数， 不定义方法
func checkOption(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must greater than 0")
	}
	return nil
}

// 从事务序列号文件，拿到最新事务序列号
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); err != nil {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
