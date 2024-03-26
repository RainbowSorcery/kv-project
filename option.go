package kv_database

type option struct {
	// 文件存储目录
	DirPath string
	// 单数据文件大小阈值
	FileDataSize int64
}
