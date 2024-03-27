package redis

import "errors"

// 通用类型存储在这个文件

// String 类型 delete 直接删除，其他类型 delete 删除的则是元数据，元数据删除后查找时元数据不存在则直接返回不存在
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (radisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, nil
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}
	// 第一个字节就是类型
	return encValue[0], nil
}
