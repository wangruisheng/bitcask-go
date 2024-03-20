package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	// tmp/a.data报错ERROR_PATH_NOT_FOUND，原因是tmp文件夹没有创建。创建tmp文件夹后通过
	// 错误很可能是您要在其中创建文件的路径不存在。它将创建日志文件，但如果父目录不存在则不会创建。 要创建必要的目录结构
	path := filepath.Join("tmp", "f.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	// defer destoryFile(path)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("tmp", "a.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	// defer destoryFile(path)

	// []byte()为强制类型转换
	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	t.Log(n, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
	t.Log(n, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("tmp", "b.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	//defer destoryFile(path)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	// t.Log(b, n)
	assert.Equal(t, n, 5)
	assert.Equal(t, []byte("key-a"), b)

	c := make([]byte, 5)
	n, err = fio.Read(c, 5)
	// t.Log(b, n)
	assert.Equal(t, n, 5)
	assert.Equal(t, []byte("key-b"), c)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("tmp", "c.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	//defer destoryFile(path)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("tmp", "d.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
