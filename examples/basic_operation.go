package main

import (
	"fmt"
	bitcask_go "myRosedb"
)

func main() {
	opts := bitcask_go.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := bitcask_go.Open(opts)
	if err != nil {
		panic(err)
	}

	// 刚开始 目录中没有dataFile会自动创建吗
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
