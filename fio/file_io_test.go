package fio

import (
	"fmt"
	"testing"
)

func TestFileIO_Read(t *testing.T) {
	fileIo, err := CreateFileIo("test.txt")
	if err != nil {
		fmt.Println(err)
	}

	_, err = fileIo.Write([]byte("hello world"))
	if err != nil {
		return
	}
}
