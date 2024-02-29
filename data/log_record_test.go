package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	// runtime error: index out of range [0] with length 0
	// 原因是const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5写错了
	res1, n1 := EncodeLogRecord(rec1) // [104 82 240 150 0 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
	t.Log(res1)                       // 编码过后的结果
	t.Log(n1)                         // 编码的长度，为 header + logRecord 的长度
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	// runtime error: index out of range [0] with length 0
	// 原因是const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5写错了
	res2, n2 := EncodeLogRecord(rec2) // [9 252 88 14 0 8 0 110 97 109 101]
	t.Log(res2)                       // 编码过后的结果
	t.Log(n2)                         // 编码的长度，为 header + logRecord 的长度
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3) // [43 153 86 17 1 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
	t.Log(n3)   // 21
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

// 不是大写的方法，所以只有相同包中能访问，Test不会提示，自己写也行
func TestDecodeLogRecordHeader(t *testing.T) {
	// 在EncodeLogRecord当中加入fmt.Printf("header length = %d, crc = %d\n", index, crc)可以看出头部长度是7
	// 所以只有前 7 个字节是头部信息，对他们进行解码即可
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	t.Log(h1)    // &{2532332136 0 4 10}
	t.Log(size1) // 7
	assert.NotNil(t, h1)
	// header length = 7
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	// value 为空的情况
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(h2)    // &{240712713 0 4 0}
	t.Log(size2) // 7
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	// 对 Deleted 情况的测试
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(h3)    // &{290887979 1 4 10}
	t.Log(size3) // 7
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc)

	// value 为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
