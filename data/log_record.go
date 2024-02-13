package data

type LogRecordType = byte

// 定义枚举类型
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	// 之前由于没大写，所以外部读取不到
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件的哪个位置
}

// 对 LogRecord 进行编码，返回字节数组以及长度
func EncodeLogRecord(logRecorrd *LogRecord) ([]byte, int64) {
	return nil, 0
}
