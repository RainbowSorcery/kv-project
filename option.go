package main

type option struct {
	// 文件存储目录
	DirPath string
	// 单数据文件大小阈值
	FileDataSize int64
}

type IteratorOption struct {
	// 是否顺序遍历
	Reverse bool

	// 遍历key为指定前缀的内容 默认为空
	Prefix []byte
}
