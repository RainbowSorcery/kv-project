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

	//for i := 0; i < 50; i++ {
	//	err = db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	//}

	for i := 0; i < 50; i++ {
		get, err := db.Get([]byte(strconv.Itoa(i)))
		fmt.Println(string(get.Key))
		fmt.Println(string(get.Value))
		if err != nil {
			panic(err)
		}
	}

}
