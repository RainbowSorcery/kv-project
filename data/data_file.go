package data

import "kv-database/fio"

type FileData struct {
	// 文件id
	FileId uint32
	// 数据写入偏移
	WriteOffset int64
	// 文件读写对象
	FileManage *fio.IOManagement
}

func (fileData *FileData) Write(data []byte, offset *int64) {

}

func (fileData *FileData) Read(pos int64) *LogRecord {
	return nil
}

func OpenFileData(path string, fileId uint32) (*FileData, error) {

	return nil, nil
}
