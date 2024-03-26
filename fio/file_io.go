package fio

import "os"

type FileIO struct {
	file *os.File
}

func Create_file_io(filePath string) (*FileIO, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &FileIO{
		file: file,
	}, nil
}

func (fileIo *FileIO) Read(buffer []byte) (int, error) {
	return fileIo.file.Read(buffer)
}

func (fileIo *FileIO) Write(buffer []byte) (int, error) {
	return fileIo.file.Write(buffer)
}

func (fileIo *FileIO) Sync() error {
	return fileIo.file.Sync()
}

func (fileIo *FileIO) Close() error {
	return fileIo.file.Close()
}
