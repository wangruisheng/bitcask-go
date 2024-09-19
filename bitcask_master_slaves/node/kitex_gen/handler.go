package main

import (
	"context"
	node "myRosedb/bitcask_master_slaves/node/kitex_gen/node"
)

// NodeServiceImpl implements the last service interface defined in the IDL.
type NodeServiceImpl struct{}

// ReplFinishNotify implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) ReplFinishNotify(ctx context.Context, req *node.ReplFinishNotifyReq) (resp bool, err error) {
	// TODO: Your code here...
	return
}

// IsAlive implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) IsAlive(ctx context.Context) (resp bool, err error) {
	// TODO: Your code here...
	return
}

// RegisterSlave implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) RegisterSlave(ctx context.Context, req *node.RegisterSlaveRequest) (resp *node.RegisterSlaveResponse, err error) {
	// TODO: Your code here...
	return
}

// PSyncReq implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReq(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	// TODO: Your code here...
	return
}

// PSyncReady implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReady(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	// TODO: Your code here...
	return
}

// OpLogEntry implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) OpLogEntry(ctx context.Context, req *node.LogEntryRequest) (resp *node.LogEntryResponse, err error) {
	// TODO: Your code here...
	return
}

// SendSlaveof implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) SendSlaveof(ctx context.Context, req *node.SendSlaveofRequest) (resp *node.SendSlaveofResponse, err error) {
	// TODO: Your code here...
	return
}

// Info implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Info(ctx context.Context) (resp *node.InfoResponse, err error) {
	// TODO: Your code here...
	return
}

// GetAllNodesInfo implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) GetAllNodesInfo(ctx context.Context, req *node.GetAllNodesInfoReq) (resp *node.GetAllNodesInfoResp, err error) {
	// TODO: Your code here...
	return
}

// Ping implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Ping(ctx context.Context) (resp *node.PingResponse, err error) {
	// TODO: Your code here...
	return
}
