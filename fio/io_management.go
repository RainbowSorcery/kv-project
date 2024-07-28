package fio

type IOManagement interface {
	// Read 文件读取 读取的内容会存储到buffer中并返回读取字节数
	Read(offset int64, buffer []byte) (int, error)
	// Write 写数据到文件中 buffer表示写入内容 返回写入字节数
	Write(buffer []byte) (int, error)

	// Sync 将缓存区内容同步到文件中
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件大小
	Size() int64

	// FileName 获取文件名称
	FileName() string

	// Remove 删除文件
	Remove() error

	// Move 移动文件
	Move(path string) error
}
