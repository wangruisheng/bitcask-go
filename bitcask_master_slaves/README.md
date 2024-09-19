package bitcask_master_slaves

### 通讯模型
- 节点之间使用rpc通讯
  - 优点
    - 功能完善，不必担心tcp命令解析等一系列问题
  - 缺点
    - 有待补充
- 结点之间直接使用tcp连接
  - 缺点
    - 实现起来繁琐且功能不足，如重连次数，超时机制都得自己实现
  - 优点
    - 实现复杂，所以简历可以吹水
    - 有部分现成的代码，bitcask单机模式下已经实现了c/s通信

### 主从模式优点：
- 读写分离，分担主节点的压力
- 负载均衡，
- 保证高可用
- 数据冗余

### 主从模式缺点：
- 主从节点之间的数据一致性问题
- 容量有限？

### 使用场景：
- 读写分离主要使用在对一致性要求不高的场景下？
- 主要使用在读多写少的场景，不然写多的话主节点压力很大，可以通过多主多从来解决

### 高性能：


redis主从分析 ：https://help.aliyun.com/document_detail/65001.html
redis主从使用 ：https://blog.csdn.net/weixin_40980639/article/details/125569460