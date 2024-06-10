package main

import "fmt"

func main() {
	db, err := open(option{
		DirPath:      "d://kv/",
		FileDataSize: 1024,
	})
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key"), []byte("value"))

	get, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(get.Key))
	fmt.Println(string(get.Value))
}
