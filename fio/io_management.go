package fio

type IOManagement interface {
	// Read 文件读取 读取的内容会存储到buffer中并返回读取字节数
	Read(buffer []byte) (int, error)
	// Write 写数据到文件中 buffer表示写入内容 返回写入字节数
	Write(buffer []byte) (int, error)

	// Sync 将缓存区内容同步到文件中
	Sync() error

	// Close 关闭文件
	Close() error
}
