package fio

import (
	"os"
	"path/filepath"
)

type FileIO struct {
	file     *os.File
	fileInfo os.FileInfo
}

func CreateFileIo(filePath string) (*FileIO, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	return &FileIO{
		file:     file,
		fileInfo: fileInfo,
	}, nil
}

func (fileIo *FileIO) Read(offset int64, buffer []byte) (int, error) {
	_, err := fileIo.file.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

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

func (fileIo *FileIO) Size() int64 {
	return fileIo.fileInfo.Size()
}

func (fileIo *FileIO) FileName() string {
	return fileIo.fileInfo.Name()
}

func (fileIo *FileIO) Remove() error {
	err := fileIo.Close()
	if err != nil {
		return err
	}
	err = os.Remove(fileIo.file.Name())

	if err != nil {
		return err
	}

	return nil
}

func (fileIo *FileIO) Move(path string) error {
	absFilePath, err := filepath.Abs(fileIo.fileInfo.Name())

	if err != nil {
		return err
	}

	err = os.Rename(absFilePath, path)
	if err != nil {
		return err
	}
	return nil
}
