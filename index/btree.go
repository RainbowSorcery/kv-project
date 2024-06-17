package index

import (
	"github.com/google/btree"
	"kv-database/data"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(64),
		lock: new(sync.RWMutex),
	}
}

func (btree *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	btree.lock.Lock()
	item := &Item{
		key: key,
		pos: pos,
	}
	btree.tree.ReplaceOrInsert(item)
	btree.lock.Unlock()

	return true
}

func (btree *Btree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}
	getItem := btree.tree.Get(item)

	if getItem == nil {
		return nil
	}

	return getItem.(*Item).pos
}

func (btree *Btree) Delete(key []byte) bool {
	btree.lock.Lock()

	item := &Item{
		key: key,
	}
	btree.tree.Delete(item)

	btree.lock.Unlock()

	return true
}

func (btree *Btree) Iterate(reverse bool) Iterator {
	btreeIterator := NewBtreeIterator(btree.tree, false)
	return btreeIterator
}
