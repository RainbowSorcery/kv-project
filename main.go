package main

func main() {
	_, err := open(option{
		DirPath:      "d://kv/",
		FileDataSize: 1024,
	})
	if err != nil {
		panic(err)
	}
	//
	//iterator := NewDbIterator(db, IteratorOption{
	//	Reverse: false,
	//	Prefix:  nil,
	//})

	//for !iterator.HasNext() {
	//	key, _ := iterator.Key()
	//	value, _ := iterator.Value()
	//
	//	fmt.Printf("key:%s value:%s\n", key, value.Value)
	//
	//	iterator.Next()
	//}

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
