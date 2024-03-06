package bitcask_go

import (
	"io"
	"myRosedb/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merger 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 如果 merge 正在进行当中，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 对当前活跃文件进行处理
	// 将当前活跃文件持久化，并进行merge，在创建新的活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转换为旧的数据文件
	db.olderFiles[db.activeFile.FileID] = db.activeFile

	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 这个活跃文件没有参与 merge，对它进行记录
	// 记录最近没有参与 merge 的文件id
	nonMergeFileId := db.activeFile.FileID

	// 	取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 将锁释放，可以接受用户新的写入了
	db.mu.Unlock()

	// 将 merge 的文件从小到大进行排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过merge，将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时 bitcask 实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	// 不用每次都 sync，因为 merge 不一定成功，最后再一起Sycn，不会影响正确性
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 如果没有更多内容可以获取，则返回
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			// 因为如果索引中有，那一定是有效的
			// 为什么不直接取出索引中的每个数据，再写入新的文件当中呢
			logRecordPos := db.index.Get(realKey)
			// 把内存中的索引位置进行比较，如果有效则重写（写入merge）
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileID &&
				logRecordPos.Offset == offset {
				// 清除事务标记，因为数据都是正确的，不需要事务号
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前（新的）位置索引写入 Hint 文件当中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 递增 offset
			offset += size
		}
	}

	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	// 写表示 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	// 标识 merge 完成了哪一部分文件的merge
	mergeFinRecord := &data.LogRecord{
		Key: []byte(mergeFinishedKey),
		// 比这个文件 id 小的，表示都参与过 merge
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// 拿到目前数据目录路径，在该目录中添加 merge 文件夹
func (db *DB) getMergePath() string {
	// path.Dir()表示拿到父目录，path.Dir()表示去除多余的斜杠
	dir := path.Dir(path.Clean(db.options.DirPath))
	// Base returns the last element of path
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath()
	// merge 目录不存在的话直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	// 如果有 merge 目录的话进行删除，因为要重写生成 merge
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找标识 merge 完成的文件，判断 merge 是否处理完成了
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果没有 merge 完成，则返回
	if !mergeFinished {
		return nil
	}

	//
	nonMergeFileId, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}
	// 删除旧的数据文件，删除比 nonMergeFileId 更小的 id 文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		// 如果数据文件存在则删掉
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据文件目录当中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		// 用 rename() 替换它
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileID(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}
	// 将 merge 完成文件中的数据取出来，因为只有一条数据，所以 offset 就是0
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引（hint采取的也是数据追加的方式，和读取数据文件方法类似）
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到实际的位置索引信息
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
