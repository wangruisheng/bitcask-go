package redis

import (
	"encoding/binary"
	"errors"
	bitcask_go "myRosedb"
	"myRosedb/index"
	"myRosedb/utils"
	"sync"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type radisDataType = byte

const (
	String radisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure Redis 数据结构数据
type RedisDataStructure struct {
	db *bitcask_go.DB
	// Redis数据结构使用的索引
	zSetIndex *zSetIndexer
}

func NewRedisDataStructure(option bitcask_go.Options) (*RedisDataStructure, error) {
	db, err := bitcask_go.Open(option)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

type zSetIndexer struct {
	mu  *sync.Mutex
	idx *index.SortedSet
}

func NewZSetIndexer() *zSetIndexer {
	return &zSetIndexer{
		mu:  new(sync.Mutex),
		idx: index.NewSortedSet(),
	}
}

// ================================ String 数据结构 ================================
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 value : type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 编码完后，使用存储引擎的接口进行写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encValue[0]
	// 判断 type
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	// 判断过期时间
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	// 返回实际value部分
	return encValue[index:], nil

}

// ================================ Hash 数据结构 ================================

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查找是否数据的key存在
	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	// 初始化 writebatch，保证元数据和数据更新的原子性
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	// 不存在元数据数量则要增加
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// 更新数据
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, nil
	}
	// 如果enckey不存在（新增value）才返回true，在enckey存在或者添加失败都返回false
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// ================================ Set 数据结构 ================================

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造一个数据部分
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err := rds.db.Get(sk.encode()); err == bitcask_go.ErrKeyNotFound {
		// 不存在的话则更新
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

// 查找member在不在对应的key里
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// 删除对应key里的member
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ================================ List 数据结构 ================================

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err := wb.Commit(); err != nil {
		return 0, nil
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分的 key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.head--
	}
	if err := rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ================================ ZSet 数据结构 ================================
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask_go.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		// 如果以前存在相应的 key + member 只是 score不同
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		// 将带score的key删掉，不然后面会被读出来
		wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64Bytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)

	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) ZRem(key []byte, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	zk.score = utils.FloatFromBytes(value)

	// 更新
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(zk.encodeWithMember())
	_ = wb.Delete(zk.encodeWithScore())
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) ZAdd2(key []byte, score float64, member []byte) (bool, error) {
	// 向磁盘追加

	// 向索引追加
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType radisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask_go.ErrKeyNotFound {
		// 如果是找不到
		exist = false
	} else {
		// 如果可以找到
		meta = decodeMetadata(metaBuf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	// 如果找不到，则创建一个新的返回
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil

}
