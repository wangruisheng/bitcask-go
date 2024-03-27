# 基于bitcask的存储引擎
## 特点
- 读写都只有一次磁盘IO，因此读写速度很快
- 写入顺序是顺序IO，保证了高吞吐
- 内存中不会存储实际的value，因此在value较大的情况下，能够处理超过内存容量的数据
- 提交日志和数据文件都是同一个文件，因此数据的崩溃恢复能够得到保证
- 备份和恢复的策略很简单，拷贝整个目录备份即可
- 相对简单易懂的代码结构和数据存储格式
# 如何开始
## 内嵌模式
```go
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

	// set a key
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	
	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```
## Redis客户端模式
### 启动服务器
```shell
cd redis/cmd
go build
./cmd
```
### 启动客户端
```shell
redis-cli
127.0.0.1:6379> hset class monitor wrs
(integer) 1
127.0.0.1:6379> sadd student1 wrs
(integer) 1
127.0.0.1:6379> lpush student wrs
(integer) 1
127.0.0.1:6379> zadd zz 1.23 a
(integer) 1
```

