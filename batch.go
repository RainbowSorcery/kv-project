package main

import (
	"encoding/binary"
	"errors"
	"kv-database/data"
	"sync"
	"sync/atomic"
)

// BatchWrite 批量原子写
type BatchWrite struct {
	// 数据库对象
	Db *Db
	// 读写锁
	Lock *sync.Mutex
	// 批量写缓存对象
	PendingWrites map[string]*data.LogRecord
}

// NewBatchWrite 构建批量原子写对象
func NewBatchWrite(db *Db) *BatchWrite {
	return &BatchWrite{
		Db:            db,
		PendingWrites: make(map[string]*data.LogRecord),
	}
}

func (batch *BatchWrite) Put(key []byte, value []byte) error {
	// 校验key是否合法
	if len(key) == 0 {
		return errors.New("empty key")
	}

	batch.Lock.Lock()
	defer batch.Lock.Unlock()

	// 添加元素到缓存批量对象中
	batch.PendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
	}

	return nil
}

func (batch *BatchWrite) Delete(key []byte) error {
	// 判断key是否合法
	if len(key) == 0 {
		return errors.New("empty key")
	}

	batch.Lock.Lock()
	defer batch.Lock.Unlock()

	// 判断key是否在批量操作对象中
	logRecord := batch.PendingWrites[string(key)]

	// 判断key是否在内存索引中
	if logRecord == nil {
		var err error
		logRecord, err = batch.Db.Get(key)
		if err != nil {
			return err
		}
		if logRecord == nil {
			return errors.New("数据不存在")
		}
	}
	// 将元素标志位设置为已删除
	logRecord.Type = data.Deleted
	batch.PendingWrites[string(key)] = logRecord

	return nil

}

func (batch *BatchWrite) Commit() error {
	batch.Lock.Lock()
	defer batch.Lock.Unlock()

	tranNum := atomic.AddUint64(batch.Db.TranNum, 1)

	for key := range batch.PendingWrites {
		record := batch.PendingWrites[key]
		if record != nil {
			batch.Db.appendLogRecord(&data.LogRecord{
				Key:   ParseTranKey(record.Key, tranNum),
				Type:  record.Type,
				Value: record.Value,
			})
		}
	}

	return nil
}

func ParseTranKey(key []byte, tranNum uint64) []byte {
	// 构造事务传输对象
	seq := make([]byte, binary.MaxVarintLen64)

	// 将事务id转换为varint类型数据
	index := binary.PutUvarint(seq, tranNum)

	// 将数据copy到目标对象中
	tranKeyByteArr := make([]byte, len(seq)+index)
	writeCount := copy(tranKeyByteArr[:index], seq[:index])
	copy(tranKeyByteArr[writeCount:], key)

	return tranKeyByteArr
}
