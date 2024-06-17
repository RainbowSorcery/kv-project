package index

import (
	"errors"
	"github.com/google/btree"
	"io"
	"kv-database/data"
)

type BtreeIterator struct {
	// 当前索引
	currentIndex int
	// 遍历顺序
	reverse bool

	// value列表
	values []*Item
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	values := make([]*Item, tree.Len())

	valueIndex := 0

	saveValues := func(item btree.Item) bool {
		values[valueIndex] = item.(*Item)
		valueIndex++

		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	newIterator := &BtreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}

	return newIterator
}

func (btreeIterator *BtreeIterator) Rewind() {
	btreeIterator.currentIndex = 0
}

func (btreeIterator *BtreeIterator) Seek(key []byte) bool {
	//TODO 先不实现 不知道方法是在什么地方用到，用到的时候再实现
	panic("implement me")
}

func (btreeIterator *BtreeIterator) Next() {
	btreeIterator.currentIndex++
}

func (btreeIterator *BtreeIterator) HasNext() bool {
	return len(btreeIterator.values) <= btreeIterator.currentIndex+1
}

func (btreeIterator *BtreeIterator) Key() ([]byte, error) {
	if len(btreeIterator.values) < btreeIterator.currentIndex {
		return nil, io.EOF
	} else {
		item := btreeIterator.values[btreeIterator.currentIndex]
		if item == nil {
			return nil, errors.New("key不存在")
		}
		return item.key, nil
	}
}

func (btreeIterator *BtreeIterator) Value() (*data.LogRecordPos, error) {
	item := btreeIterator.values[btreeIterator.currentIndex]
	return item.pos, nil
}

func (btreeIterator *BtreeIterator) Close() error {
	btreeIterator.values = nil
	return nil
}
