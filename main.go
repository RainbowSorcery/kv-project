package main

import (
	"fmt"
	"strconv"
)

func main() {
	db, err := open(option{
		DirPath:      "d://kv/",
		FileDataSize: 1024,
	})
	if err != nil {
		panic(err)
	}
	//
	//for i := 0; i < 1000; i++ {
	//	err = db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	//}

	//db.Merge()

	//
	for i := 0; i < 1000; i++ {
		get, err := db.Get([]byte(strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
		fmt.Println(string(get.Value))
	}

}
