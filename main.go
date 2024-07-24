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

	index := 10000
	//creatData(db, index)

	getData(db, index)

}

func creatData(db *Db, index int) {
	for i := 0; i < index; i++ {
		_ = db.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
}
func getData(db *Db, index int) {
	for i := 0; i < index; i++ {
		get, err := db.Get([]byte(strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
		fmt.Println(string(get.Value))
	}
}
