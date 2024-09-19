package bitcask_master_slaves

import (
	"context"
	"myRosedb/bitcask_master_slaves/idl/gen-go/node"
)

// NodeServiceImpl implements the last service interface defined in the IDL.
// 实现在 IDL 当中定义的 node 接口
type NodeServiceImpl struct{}

// PSync implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSync(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReq(req)
}

// Ping implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Ping(ctx context.Context) (resp *node.PingResponse, err error) {
	return &node.PingResponse{Status: true}, nil
}

// Info implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Info(ctx context.Context) (resp *node.InfoResponse, err error) {
	return &node.InfoResponse{
		Role:                    config.RoleNameMap[bitcaskNode.GetConfig().Role],
		ConnectedSlaves:         int64(bitcaskNode.GetConfig().ConnectedSlaves),
		MasterReplicationOffset: int64(bitcaskNode.GetConfig().MasterReplicationOffset),
		CurReplicationOffset:    int64(bitcaskNode.GetConfig().CurReplicationOffset),
	}, nil
}

// SendSlaveof implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) SendSlaveof(ctx context.Context, req *node.SendSlaveofRequest) (resp *node.SendSlaveofResponse, err error) {
	return bitcaskNode.SendSlaveOfReq(req)
}

// RegisterSlave implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) RegisterSlave(ctx context.Context, req *node.RegisterSlaveRequest) (resp *node.RegisterSlaveResponse, err error) {
	return bitcaskNode.HandleSlaveOfReq(req)
}

// OpLogEntry implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) OpLogEntry(ctx context.Context, req *node.LogEntryRequest) (resp *node.LogEntryResponse, err error) {
	return bitcaskNode.HandleOpLogEntryRequest(req)
}

// // IncrReplFailNotify implements the NodeServiceImpl interface.
// func (s *NodeServiceImpl) IncrReplFailNotify(ctx context.Context, masterId string) (resp bool, err error) {
// 	return bitcaskNode.HandleRepFailNotify(masterId)
// }

// ReplFinishNotify implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) ReplFinishNotify(ctx context.Context, req *node.ReplFinishNotifyReq) (resp bool, err error) {
	return bitcaskNode.HandleReplFinishNotify(req)
}

// PSyncReq implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReq(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReq(req)
}

// PSyncReady implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReady(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReady(req)
}

// GetAllNodesInfo implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) GetAllNodesInfo(ctx context.Context, req *node.GetAllNodesInfoReq) (resp *node.GetAllNodesInfoResp, err error) {
	return bitcaskNode.GetAllNodesInfo(req)
}

// IsAlive implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) IsAlive(ctx context.Context) (resp bool, err error) {
	return true, nil
}
