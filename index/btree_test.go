package index

import (
	"fmt"
	"os"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	//bt := NewBtree()
	//bt.Put([]byte("hello"), &data.LogRecordPos{
	//	FileId: 12,
	//	Pos: 1,
	//})
	//
	//get := bt.Get([]byte("hello"))
	//
	//fmt.Println(get)

	file, err := os.OpenFile("d://test.txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
	}

	writeString, err := file.WriteString("hello world")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(writeString)
	file.Sync()
}
