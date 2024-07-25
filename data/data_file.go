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
	if fileData == nil {
		return nil, 0, errors.New("fileData is nil")
	}

	buffer, err := fileData.readNByte(pos, logRecordLengthSize)
	if err != nil {
		return nil, 0, err
	}

	recordHeader, size := DecodingLogRecordHeader(buffer)
	headerSize := size
	// 判断是否到达文件末尾，如果读取不到header数据表示到末尾了
	if recordHeader == nil {
		return nil, 0, io.EOF
	}

	if recordHeader.Crc == 0 && recordHeader.KeySize == 0 && recordHeader.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// 读取key value数据
	pos += size

	recordDataBuffer, err := fileData.readNByte(pos, int64(recordHeader.KeySize+recordHeader.ValueSize))
	if err != nil {
		return nil, 0, err
	}

	logRecord := &LogRecord{
		Key:   recordDataBuffer[:recordHeader.KeySize],
		Value: recordDataBuffer[recordHeader.KeySize : recordHeader.KeySize+recordHeader.ValueSize],
		Type:  recordHeader.Type,
	}

	// crc冗余校验
	crc := GetLogRecordCRC(logRecord, buffer[crc32.Size:headerSize])

	if crc != recordHeader.Crc {
		return nil, 0, errors.New("crc校验失败")
	}

	return logRecord, headerSize + int64(recordHeader.KeySize+recordHeader.ValueSize), nil
}

func (fileData *FileData) readNByte(pos int64, length int64) ([]byte, error) {
	buffer := make([]byte, length)
	_, err := fileData.FileManage.Read(pos, buffer)

	if err != nil {
		return nil, err
	}

	return buffer, nil
}

// ReadLogRecord 根据偏移获取logRecord
func (fileData *FileData) ReadLogRecord(pos int64) (logRecord *LogRecord, err error) {
	headerDataBuffer, err := fileData.readNByte(pos, 13)
	if err != nil {
		return nil, err
	}

	// 根据最长长度解码header
	header, size := DecodingLogRecordHeader(headerDataBuffer)

	if header == nil {
		return nil, errors.New("header为空")
	}

	// 拿到header之后就可以获取到logRecord的值了
	recordByteArray, err := fileData.readNByte(pos+size, int64(header.KeySize+header.ValueSize))
	if err != nil {
		return nil, err
	}

	logRecord = &LogRecord{
		Key:   recordByteArray[:header.KeySize],
		Value: recordByteArray[header.KeySize : header.ValueSize+header.KeySize],
		Type:  header.Type,
	}

	return logRecord, nil
}

func (fileData *FileData) WriteHintRecord(pos *LogRecordPos) error {

	recordPos, err := EncodingLogRecordPos(pos)

	if err != nil {
		return err
	}

	err = fileData.Write(recordPos)
	if err != nil {
		return err
	}

	return nil
}

func (fileData *FileData) WriteMergeFinishRecord(mergeRecord *MergeFinishRecord) error {
	mergeRecordBytesArr := make([]byte, 16)

	writeIndex := 0
	writeIndex += binary.PutVarint(mergeRecordBytesArr[writeIndex:], int64(mergeRecord.FinishCount))
	for fileId := range mergeRecord.mergerFinishFileIds {
		writeIndex += binary.PutVarint(mergeRecordBytesArr[writeIndex:], int64(fileId))
	}

	err := fileData.Write(mergeRecordBytesArr)
	if err != nil {
		return err
	}

	return nil
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

func OpenHintFile(path string) (*FileData, error) {
	// 拼接路径
	dataFilePath := path + "hint-index" + ".hint"
	// 创建IOManagement对象
	fileIo, err := fio.CreateFileIo(dataFilePath)
	if err != nil {
		return nil, err
	}

	// 创建FileData对象

	return &FileData{
		WriteOffset: 0,
		FileManage:  fileIo,
	}, nil
}

func OpenFinishMergeFile(path string) (*FileData, error) {
	// 拼接路径
	dataFilePath := path + "merge-fin" + ".fin"
	// 创建IOManagement对象
	fileIo, err := fio.CreateFileIo(dataFilePath)
	if err != nil {
		return nil, err
	}

	// 创建FileData对象

	return &FileData{
		WriteOffset: 0,
		FileManage:  fileIo,
	}, nil
}
