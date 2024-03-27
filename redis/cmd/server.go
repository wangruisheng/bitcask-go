package main

import (
	"github.com/tidwall/redcon"
	"log"
	bitcask "myRosedb"
	bitcask_redis "myRosedb/redis"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	// 在 redis 当中可以对应多个db，用select切换到不同的db上，所以用map存储
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()

}

// 启动监听服务，监听客户端的连接
func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections")
	svr.server.ListenAndServe()
}

// 有新的连接进来，需要进行处理
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	// 初始化客户端？？？
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	// 默认使用第一个数据库
	cli.db = svr.dbs[0]
	// 先放到context里，对客户端命令进行执行的时候从context中取出来
	conn.SetContext(cli)
	return true
}

// 断开连接后的处理
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

// redis 协议解析的示例
//func main() {
//	conn, err := net.Dial("tcp", "localhost:6379")
//	if err != nil {
//		panic(err)
//	}
//
//	// 向Redis发送一个命令
//	cmd := "set k-name-2 bitcask-kv-2\r\n"
//	conn.Write([]byte(cmd))
//
//	// 解析 Redis 响应
//	reader := bufio.NewReader(conn)
//	res, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(res)
//
//}
