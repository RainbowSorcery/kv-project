package index

import "kv-database/data"

type Iterator interface {
	// Rewind 回到迭代器起点
	Rewind()

	// Seek 根据key所在位置继续遍历
	Seek(key []byte) bool

	// Next 遍历下一个key
	Next()

	// HasNext 判断是否有下一个key用于遍历
	HasNext() bool

	// Key 获取当前迭代器所在位置的key
	Key() ([]byte, error)

	// Value 获取当前迭代器所在位置的value
	Value() (*data.LogRecordPos, error)

	// Close 关闭迭代器 是否资源
	Close() error
}
