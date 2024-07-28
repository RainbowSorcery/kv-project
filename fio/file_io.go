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

func (fileIO *FileIO) Read(offset int64, buffer []byte) (int, error) {
	_, err := fileIO.file.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return fileIO.file.Read(buffer)
}

func (fileIO *FileIO) Write(buffer []byte) (int, error) {
	return fileIO.file.Write(buffer)
}

func (fileIO *FileIO) Sync() error {
	return fileIO.file.Sync()
}

func (fileIO *FileIO) Close() error {
	return fileIO.file.Close()
}

func (fileIO *FileIO) Size() int64 {
	return fileIO.fileInfo.Size()
}

func (fileIO *FileIO) FileName() string {
	return fileIO.fileInfo.Name()
}

func (fileIO *FileIO) Remove() error {
	err := fileIO.Close()
	if err != nil {
		return err
	}
	err = os.Remove(fileIO.file.Name())

	if err != nil {
		return err
	}

	return nil
}

func (fileIO *FileIO) Move(path string) error {
	absFilePath, err := filepath.Abs(fileIO.fileInfo.Name())

	if err != nil {
		return err
	}

	err = os.Rename(absFilePath, path)
	if err != nil {
		return err
	}
	return nil
}

func (fileIO *FileIO) Exits() bool {
	_, err := os.Stat(fileIO.FileName())
	if os.IsNotExist(err) {
		return false
	}
	return true
}
