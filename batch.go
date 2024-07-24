package main

import (
	"encoding/binary"
	"errors"
	"kv-database/data"
	"sync"
	"sync/atomic"
)

// TxComPrefix 事务完成前缀
const TxComPrefix = "tx_com_prefix"

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
		Lock:          &sync.Mutex{},
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
		Type:  data.Normal,
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

	tranNum := atomic.AddInt64(batch.Db.TranNum, 1)

	logRecordPositionMap := make(map[string]*data.LogRecordPos)

	for key := range batch.PendingWrites {
		record := batch.PendingWrites[key]
		if record != nil {
			_, realKey := DecodingTranKey(record.Key)
			position, err := batch.Db.appendLogRecord(&data.LogRecord{
				Key:   EncodingTranKey(realKey, tranNum),
				Type:  record.Type,
				Value: record.Value,
			})

			// 记录索引偏移信息 以便于保存到硬盘中
			logRecordPositionMap[string(record.Key)] = position
			if err != nil {
				return err
			}
		}
	}

	// 所有记录追加到磁盘后需要添加一条记录用于表示事务写完成
	txCompRecord := &data.LogRecord{
		Key:   EncodingTranKey([]byte(TxComPrefix), tranNum),
		Value: nil,
		Type:  data.TxComplete,
	}

	txCompRecordPos, err := batch.Db.appendLogRecord(txCompRecord)
	if err != nil {
		return err
	}

	logRecordPositionMap[string(txCompRecord.Key)] = txCompRecordPos

	// 强制刷盘
	err = batch.Db.activeFile.FileManage.Sync()
	if err != nil {
		return err
	}

	// 将更改的索引信息刷新到内存中 判断索引是否被删除如果被删除则删除内存索引否则则添加内存索引
	for key := range batch.PendingWrites {
		record := batch.PendingWrites[key]
		pos := logRecordPositionMap[key]
		if record.Type == data.Normal {
			batch.Db.index.Put([]byte(key), pos)
		} else if record.Type == data.Deleted {
			batch.Db.index.Delete([]byte(key))
		}
	}

	return nil
}

func EncodingTranKey(key []byte, tranNum int64) []byte {
	// 构造事务传输对象
	seq := make([]byte, binary.MaxVarintLen64)

	// 将事务id转换为variant类型数据
	index := binary.PutVarint(seq, tranNum)

	// 将数据copy到目标对象中
	tranKeyByteArr := make([]byte, len(key)+index)
	writeCount := copy(tranKeyByteArr[:index], seq[:index])
	copy(tranKeyByteArr[writeCount:], key)

	return tranKeyByteArr
}

func DecodingTranKey(key []byte) (int64, []byte) {
	seq, size := binary.Varint(key)

	return seq, key[size:]
}
