package main

import (
	"log"
	node "myRosedb/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
)

func main() {
	svr := node.NewServer(new(NodeServiceImpl))

	err := svr.Run()

	if err != nil {
		log.Println(err.Error())
	}
}
