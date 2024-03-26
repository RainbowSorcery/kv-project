package fio

import (
	"fmt"
	"testing"
)

func TestFileIO_Read(t *testing.T) {
	fileIo, err := Create_file_io("test.txt")
	if err != nil {
		fmt.Println(err)
	}

	fileIo.Write([]byte("hello world"))
}
