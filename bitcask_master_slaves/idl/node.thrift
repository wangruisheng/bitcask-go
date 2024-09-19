namespace go node

// 定义错误代码
enum ErrCode {
    SuccessCode       = 0
    ServiceErrCode    = 10001
    ParamErrCode      = 10002
    SlaveofErrCode    = 10003
    OpLogEntryErrCode = 10004
}

// 定义响应
struct BaseResp {
    1: i64 status_code
    2: string status_message
    3: i64 service_time
}

// slaves向master发送注册请求
struct RegisterSlaveRequest {
    1: string address   // 从节点地址
    2: string runId  // 从节点的唯一标识
    3: i32 weight //权重
}

// master的响应
struct RegisterSlaveResponse {
    1: BaseResp base_resp
    2: string runId // 主节点的唯一标识
    3: i64 offset // 服务器的进度
}

# client要求所连node发送RegisterSlave请求？？？客户端请求某个节点成为指定主节点的从节点，响应仅包含基础响应结构。
struct SendSlaveofRequest {
    1: string address   // 目标Master地址
}

struct SendSlaveofResponse {
    1: BaseResp base_resp
}

# 数据传输
enum OperationCode {
    Insert = 0
    Delete = 1
    Query = 2
}

# LogRecord部分
struct LogEntry {
    1: string key
    2: string value
    3: i64 score
    4: i64 expireAt
}

// PSyncRequest 是从节点向主节点发出的同步请求，包含主节点 ID、从节点 ID 和当前同步的进度偏移量。
// PSyncResponse 返回同步的状态码，指示下一步操作。
// 数据更新请求（从节点主动发出）
struct PSyncRequest {
    1: string master_id
    2: string slave_id
    3: i64 offset   // ！！！从节点的复制进度，如果为-1则表示全量复制，否则为增量复制，若master判断无法满足增量复制条件，则开始进行全量复制
}

struct PSyncResponse {
    1: i8 code  //从节点根据状态码，判断接下来应该增量复制还是全量复制，并设置offset、syncstatus等字段信息
    # 2: LogEntry entry
}

// 定义了一个数据操作请求，主节点用来执行指定的操作（增删改查），响应包含操作结果的日志条目列表或状态信息。
struct LogEntryRequest {
    1: i64 entry_id // 客户端发起的请求不会有这个标识，主节点发送请求时会有这个标识，方便进度同步
    2: string cmd
    3: list<string> args
    4: string master_id
}

struct LogEntryResponse {
    1: BaseResp base_resp
    2: list<LogEntry> entries
    3: string info // 若状态码正确且entries为空，则输出info信息
}

# 健康检测，检测节点是否在线
struct PingRequest {
    1: bool ping
}

struct PingResponse {
    1: bool status
}

# 节点信息打印
# 用于获取节点状态和配置信息，如角色、连接的从节点数量、主节点的复制偏移量等。
struct InfoRequest {
    1: bool ping
}

# 通知从节点复制完成，包含同步类型、完成状态、主节点偏移量和最后一个日志条目 ID。
struct InfoResponse {
    1: string role
    2: i64 connected_slaves
    3: i64 master_replication_offset
    4: i64 cur_replication_offset
}

# 通知从节点复制完成，包含同步类型、完成状态、主节点偏移量和最后一个日志条目 ID。
struct ReplFinishNotifyReq {
    1: i8 sync_type
    2: bool ok
    3: i64 master_offset
    4: i64 last_entry_id    // 供slave在全量复制时校验用
}

struct GetAllNodesInfoReq {

}

struct SlaveInfo {
    1: string addr
    2: string id
    3: i32 weight
}

# 请求获取所有节点的信息，响应包含所有从节点的详细信息和最后更新时间。
struct GetAllNodesInfoResp {
    1: list<SlaveInfo> infos
    2: i64 lastUpdateTime
}

service NodeService {
    # master -> slave 主节点向从节点发送复制完成通知
    bool ReplFinishNotify(1: ReplFinishNotifyReq req)
    # 用于检测节点是否在线
    bool IsAlive()

    # 注册从节点到主节点
    # 从节点 向 主节点 发请求
    # slave -> master
    RegisterSlaveResponse RegisterSlave(1: RegisterSlaveRequest req)
    PSyncResponse PSyncReq(1: PSyncRequest req) // slave 发起请求
    PSyncResponse PSyncReady(1: PSyncRequest req) // slave 告知 master 已经准备好

    # 客户端 向 主/从节点 发请求
    # client -> master/slave
    # 主节点 向 客户端 发请求
    # master -> client
    # 代理节点 向 主/从节点 发请求
    # proxy -> client/master
    # client 对数据库的请求以及响应
    LogEntryResponse OpLogEntry(1: LogEntryRequest req)

    # 客户端 向 主节点 发请求
    # client -> master
    SendSlaveofResponse SendSlaveof(1: SendSlaveofRequest req) # 客户端要求某节点成为指定节点的slave
    InfoResponse Info() # 客户端获取节点的信息

    # 代理节点 向 主节点 发请求
    # proxt -> master
    # 获取所有的节点信息
    GetAllNodesInfoResp GetAllNodesInfo(1: GetAllNodesInfoReq req)
    # proxy -> slave
    # master -> slave
    # 发送心跳检测
    PingResponse Ping()
}