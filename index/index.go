package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-database/data"
)

type Indexer interface {
	// Put 设置索引到内存中
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 获取索引信息
	Get(key []byte) *data.LogRecordPos
	// Delete 删除索引
	Delete(key []byte) bool

	// Iterate 获取迭代器
	Iterate(reverse bool) Iterator
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(than btree.Item) bool {
	// (*Item).key表示断言 如果than实现了btree.Item那么就转换成Item
	return bytes.Compare(item.key, than.(*Item).key) == -1
}
