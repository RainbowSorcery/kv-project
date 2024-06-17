package main

import (
	"fmt"
)

func main() {
	db, err := open(option{
		DirPath:      "d://kv/",
		FileDataSize: 1024,
	})
	if err != nil {
		panic(err)
	}

	iterate := db.index.Iterate(true)

	for !iterate.HasNext() {
		iterate.Next()
		fmt.Println(iterate.Value())
	}

	//
	//for i := 0; i < 1000; i++ {
	//	err = db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	//}
	//
	//for i := 0; i < 1000; i++ {
	//	get, err := db.Get([]byte(strconv.Itoa(i)))
	//	if err != nil {
	//		panic(err)
	//	}
	//	fmt.Println(string(get.Key))
	//	fmt.Println(string(get.Value))
	//}

}
