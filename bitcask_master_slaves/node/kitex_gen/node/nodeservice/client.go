// Code generated by Kitex v0.11.3. DO NOT EDIT.

package nodeservice

import (
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
	"myRosedb/bitcask_master_slaves/node/kitex_gen/node"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	ReplFinishNotify(ctx context.Context, req *node.ReplFinishNotifyReq, callOptions ...callopt.Option) (r bool, err error)
	IsAlive(ctx context.Context, callOptions ...callopt.Option) (r bool, err error)
	RegisterSlave(ctx context.Context, req *node.RegisterSlaveRequest, callOptions ...callopt.Option) (r *node.RegisterSlaveResponse, err error)
	PSyncReq(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error)
	PSyncReady(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error)
	OpLogEntry(ctx context.Context, req *node.LogEntryRequest, callOptions ...callopt.Option) (r *node.LogEntryResponse, err error)
	SendSlaveof(ctx context.Context, req *node.SendSlaveofRequest, callOptions ...callopt.Option) (r *node.SendSlaveofResponse, err error)
	Info(ctx context.Context, callOptions ...callopt.Option) (r *node.InfoResponse, err error)
	GetAllNodesInfo(ctx context.Context, req *node.GetAllNodesInfoReq, callOptions ...callopt.Option) (r *node.GetAllNodesInfoResp, err error)
	Ping(ctx context.Context, callOptions ...callopt.Option) (r *node.PingResponse, err error)
}

// NewClient creates a client for the service defined in IDL.
func NewClient(destService string, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithDestService(destService))

	options = append(options, opts...)

	kc, err := client.NewClient(serviceInfoForClient(), options...)
	if err != nil {
		return nil, err
	}
	return &kNodeServiceClient{
		kClient: newServiceClient(kc),
	}, nil
}

// MustNewClient creates a client for the service defined in IDL. It panics if any error occurs.
func MustNewClient(destService string, opts ...client.Option) Client {
	kc, err := NewClient(destService, opts...)
	if err != nil {
		panic(err)
	}
	return kc
}

type kNodeServiceClient struct {
	*kClient
}

func (p *kNodeServiceClient) ReplFinishNotify(ctx context.Context, req *node.ReplFinishNotifyReq, callOptions ...callopt.Option) (r bool, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ReplFinishNotify(ctx, req)
}

func (p *kNodeServiceClient) IsAlive(ctx context.Context, callOptions ...callopt.Option) (r bool, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.IsAlive(ctx)
}

func (p *kNodeServiceClient) RegisterSlave(ctx context.Context, req *node.RegisterSlaveRequest, callOptions ...callopt.Option) (r *node.RegisterSlaveResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.RegisterSlave(ctx, req)
}

func (p *kNodeServiceClient) PSyncReq(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.PSyncReq(ctx, req)
}

func (p *kNodeServiceClient) PSyncReady(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.PSyncReady(ctx, req)
}

func (p *kNodeServiceClient) OpLogEntry(ctx context.Context, req *node.LogEntryRequest, callOptions ...callopt.Option) (r *node.LogEntryResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.OpLogEntry(ctx, req)
}

func (p *kNodeServiceClient) SendSlaveof(ctx context.Context, req *node.SendSlaveofRequest, callOptions ...callopt.Option) (r *node.SendSlaveofResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.SendSlaveof(ctx, req)
}

func (p *kNodeServiceClient) Info(ctx context.Context, callOptions ...callopt.Option) (r *node.InfoResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Info(ctx)
}

func (p *kNodeServiceClient) GetAllNodesInfo(ctx context.Context, req *node.GetAllNodesInfoReq, callOptions ...callopt.Option) (r *node.GetAllNodesInfoResp, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.GetAllNodesInfo(ctx, req)
}

func (p *kNodeServiceClient) Ping(ctx context.Context, callOptions ...callopt.Option) (r *node.PingResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Ping(ctx)
}
