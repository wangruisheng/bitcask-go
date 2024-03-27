package benchmark

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	bitcask_go "myRosedb"
	"myRosedb/redis"
	"myRosedb/utils"
	"os"
	"testing"
	"time"
)

var rds *redis.RedisDataStructure

func init() {
	// 初始化用于基准测试的存储引擎
	options := bitcask_go.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench-redis")
	options.DirPath = dir

	var err error
	rds, err = redis.NewRedisDataStructure(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Set(b *testing.B) {
	// 将计时器重置
	b.ResetTimer()
	// 打印出内存分配情况
	b.ReportAllocs()

	// 结果：					执行次数		每一次执行耗时			每次内存分配情况			每次操作分配了10次内存
	// Benchmark_Set_24   	   166320	     6884 ns/op	    5828 B/op	      11 allocs/op
	for i := 0; i < b.N; i++ {
		// 将value设置为1kb
		err := rds.Set(utils.GetTestKey(i), 0, utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

// 结果：							执行次数		每一次执行耗时			每次内存分配情况			每次操作分配了10次内存
// BenchmarkRedis_Get-24         5104326       234.7 ns/op         135						4 allocs/op
func BenchmarkRedis_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := rds.Set(utils.GetTestKey(i), 0, utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := rds.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask_go.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
