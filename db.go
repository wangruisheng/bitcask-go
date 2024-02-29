package bitcask_go

import (
	"errors"
	"io"
	"myRosedb/data"
	"myRosedb/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 这个文件主要存放面相用户的操作接口

// DB bitcask 存储引擎
type DB struct {
	options    Options
	fileIds    []int
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Open 打开存储引擎实例 bitcask
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOption(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在的话，就创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options: options,
		mu:      new(sync.RWMutex),
		//activeFile: new(data.DataFile),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载对应的数据文件
	if err := db.OpenDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// 写入 Key/Value 数据，Key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 先判断 key 是否无效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加数据写入磁盘文件
	pos, err := db.appendLogRecord(logRecord)
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
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord，标识是被删除的
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// 写入到数据文件当中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中将对应的 key删除
	// 为什么老师的代码只有一个返回值
	_, ok := db.index.Delete(key)
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
	var dataFile *data.DataFile
	if db.activeFile.FileID == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, err
	}

	return logRecord.Value, nil
}

// 定义 LogRecord 写入磁盘方法，方法不用大写，因为是内部方法
// 返回内存索引信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()

	defer db.mu.Unlock()

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

	// 看用户是否每次进行写入后都想要进行持久化，根据用户配置决定
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) OpenDataFiles() error {
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
			// 包strconv实现了与基本数据类型的字符串表示形式之间的转换，Atoi相当于ParseInt（s,10,0），转换为int类型。
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
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
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

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
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
			if logRecord.Type == data.LogRecordDeleted {
				// 都没加为什么要删？
				// 因为在重启数据库的时候，如果我们不对已删除的数据进行处理的话，内存索引是不会知道的，那么被删除的数据对应的索引仍然存在，会导致已经被删除的数据又存在了，数据会发生不一致
				// 所以在启动 bitcask 实例，从数据文件加载索引的时候，需要对已删除的记录进行处理（因为数据文件中，同一个key的被删除记录总在加入记录之后，所以查找到删除type的时候，这个	key 之前肯定被加进来过）
				// 如果判断到当前处理的记录是已删除的，则根据对应的key将内存索引中的数据删除
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增 offset， 下一次从新的位置开始
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

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
