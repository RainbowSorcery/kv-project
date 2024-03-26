package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kv-database/fio"
)

type FileData struct {
	// 文件id
	FileId uint32
	// 数据写入偏移
	WriteOffset int64
	// 文件读写对象
	FileManage fio.IOManagement
}

func (fileData *FileData) Write(data []byte) error {
	writeSize, err := fileData.FileManage.Write(data)

	if err != nil {
		return err
	}

	// 更新文件写入偏移
	fileData.WriteOffset = fileData.WriteOffset + int64(writeSize)

	return nil
}

func (fileData *FileData) Read(pos int64) (*LogRecord, int64, error) {
	// 读取header header中存储crc冗余校验、record类型、key长度、value长度
	logRecordLengthSize := int64(binary.MaxVarintLen32*3 + 1)
	buffer, err := fileData.readNByte(pos, logRecordLengthSize)
	if err != nil {
		return nil, 0, err
	}

	recordHeader := DecodingLogRecordHeader(buffer)
	// 判断是否到达文件末尾，如果读取不到header数据旧表示到末尾了
	if recordHeader == nil {
		return nil, 0, io.EOF
	}

	if recordHeader.Crc == 0 && recordHeader.KeySize == 0 && recordHeader.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// 读取key value数据
	pos += logRecordLengthSize

	recordDataBuffer, err := fileData.readNByte(pos, int64(recordHeader.KeySize+recordHeader.ValueSize))
	if err != nil {
		return nil, 0, err
	}

	logRecord := &LogRecord{
		Key:   recordDataBuffer[:recordHeader.KeySize],
		Value: recordDataBuffer[:recordHeader.ValueSize],
		Type:  recordHeader.Type,
	}

	// crc冗余校验
	crc := GetLogRecordCRC(logRecord, buffer[crc32.Size:logRecordLengthSize])

	if crc != recordHeader.Crc {
		return nil, 0, errors.New("crc校验失败")
	}

	return logRecord, int64(binary.MaxVarintLen32*3 + 1 + recordHeader.KeySize + recordHeader.ValueSize), nil
}

func (fileData *FileData) readNByte(pos int64, length int64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := fileData.FileManage.Read(pos, buffer)

	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func OpenFileData(path string, fileId uint32) (*FileData, error) {
	// 拼接路径
	dataFilePath := path + fmt.Sprintf("%09d", fileId) + ".data"
	// 创建IOManagement对象
	fileIo, err := fio.CreateFileIo(dataFilePath)
	if err != nil {
		return nil, err
	}

	// 创建FileData对象

	return &FileData{
		FileId:      fileId,
		WriteOffset: 0,
		FileManage:  fileIo,
	}, nil
}
