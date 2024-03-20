package data

import (
	"github.com/stretchr/testify/assert"
	"myRosedb/fio"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	//fmt.Println(os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	//fmt.Println(os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	//fmt.Println(os.TempDir())
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(os.TempDir())

}

func TestDataFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("ccc"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile2, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	err = dataFile2.Close()
	assert.Nil(t, err)

}

func TestDataFile_Synca(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("fff"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 333, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	// 先编码
	// 刚开始报错，是因为logRecord.Value = kvBuf[keySize:]写成logRecord.Value = kvBuf[:valueSize]
	res1, size1 := EncodeLogRecord(rec1)
	// 再写入
	// 写入的时候是乱码
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(24)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	t.Log(size3)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)

}
