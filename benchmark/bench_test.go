package benchmark

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	bitcask_go "myRosedb"
	"myRosedb/utils"
	"os"
	"testing"
	"time"
)

/*
goos: linux
goarch: amd64
pkg: myRosedb/benchmark
cpu: AMD EPYC 7543 32-Core Processor
Benchmark_Put-32           68312             18487 ns/op            4680 B/op         10 allocs/op
Benchmark_Get-32         1923130               681.3 ns/op           135 B/op          4 allocs/op
Benchmark_Delete-32      1875025               644.4 ns/op           135 B/op          4 allocs/op
*/
/*
goos: windows
goarch: amd64
pkg: myRosedb/benchmark
cpu: 13th Gen Intel(R) Core(TM) i7-13700KF
Benchmark_Put-24          158560              7171 ns/op            4676 B/op		  10 allocs/op
Benchmark_Get-24         4567041               260.8 ns/op           135 B/op		   4 allocs/op
Benchmark_Delete-24      4618396               255.2 ns/op           135 B/op		   4 allocs/op
*/
var db *bitcask_go.DB

func init() {
	// 初始化用于基准测试的存储引擎
	options := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	// dir, _ := os.MkdirTemp("", "bitcask-go-bench-redis")
	options.DirPath = dir

	var err error
	db, err = bitcask_go.Open(options)
	// rds, err := redis.NewRedisDataStructure(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	// 将计时器重置
	b.ResetTimer()
	// 打印出内存分配情况
	b.ReportAllocs()

	// 结果：					执行次数		每一次执行耗时			每次内存分配情况			每次操作分配了10次内存
	// Benchmark_Put-32    	   58300	     20581 ns/op	    4674 B/op	      10 allocs/op
	for i := 0; i < b.N; i++ {
		// 将value设置为1kb
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask_go.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	// 将计时器重置
	b.ResetTimer()
	// 打印出内存分配情况
	b.ReportAllocs()

	rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		// 将value设置为1kb
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
