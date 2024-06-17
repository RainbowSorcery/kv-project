package main

import (
	"kv-database/data"
	"kv-database/index"
)

type DbIterator struct {
	// 数据库文件
	Db *Db
	// 配置信息
	Option IteratorOption
	// 迭代器
	IndexIterator index.Iterator
}

func NewDbIterator(db *Db, option IteratorOption) *DbIterator {
	iterate := db.index.Iterate(option.Reverse)
	return &DbIterator{
		Db:            db,
		Option:        option,
		IndexIterator: iterate,
	}
}

// Rewind 回到迭代器起点
func (dbIterator *DbIterator) Rewind() {
	dbIterator.IndexIterator.Rewind()
}

// Next 遍历下一个key
func (dbIterator *DbIterator) Next() {
	dbIterator.IndexIterator.Next()
}

// HasNext 判断是否有下一个key用于遍历
func (dbIterator *DbIterator) HasNext() bool {
	return dbIterator.IndexIterator.HasNext()
}

// Key 获取当前迭代器所在位置的key
func (dbIterator *DbIterator) Key() ([]byte, error) {
	return dbIterator.IndexIterator.Key()
}

// Value 获取当前迭代器所在位置的value
func (dbIterator *DbIterator) Value() (*data.LogRecord, error) {
	key, err := dbIterator.IndexIterator.Key()
	if err != nil {
		return nil, err
	}
	get, err := dbIterator.Db.Get(key)
	if err != nil {
		return nil, err
	}
	return get, nil
}

// Close 关闭迭代器 是否资源
func (dbIterator *DbIterator) Close() error {
	return dbIterator.IndexIterator.Close()
}
