package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-database/data"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // 设置值
	Get(key []byte) *data.LogRecordPos           // 获取数据的位置信息
	Delete(key []byte) bool                      // 删除kv数据
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (item *Item) Less(than btree.Item) bool {
	// (*Item).key表示断言 如果than实现了btree.Item那么就转换成Item
	return bytes.Compare(item.key, than.(*Item).key) == -1
}
