package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	Deleted = 0
	Normal  = 1
)

// LogRecordPos 数据内存索引信息 主要是根据key找到指定文件的指定位置读取指定数据
type LogRecordPos struct {
	FileId uint32 // 文件id
	Pos    int64  // 数据偏移
}

type LogRecordHeader struct {
	Crc       uint32
	Type      LogRecordType
	KeySize   uint32
	ValueSize uint32
}

type LogRecord struct {
	// key
	Key []byte
	// value
	Value []byte
	// 索引是否删除
	Type LogRecordType
}

// EncodingLogRecord 将record对象实例化为字节数组并返回长度以及序列化后的对象结果
func EncodingLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, 15)

	// 前3个字节为crc冗余校验位，该位等整个LogRecord读取出来才能进行计算，所以需要先跳过前三个字节，从第四个字节开始设置
	var index = 4
	header[index] = logRecord.Type
	index++

	keySize := len(logRecord.Key)
	valueSize := len(logRecord.Value)
	// 写入字节数值到header中 PutVarint会返回每次写入字节数 因为keySize和valueSize不是定长的，所以需要这样设置一些
	index += binary.PutVarint(header[index:], int64(keySize))
	index += binary.PutVarint(header[index:], int64(valueSize))

	// 计算logRecord长度 header长度 + key长度 + value长度
	var size = int64(index + keySize + valueSize)

	logRecordByteArray := make([]byte, size)

	// 将header数据拷贝到logRecordByteArray中
	copy(logRecordByteArray[:index], header[:index])
	// 将key value设置到字节数组中 因为key value存储的就是字节数组所以不需要编解码 直接设置即可
	copy(logRecordByteArray[index:], logRecord.Key)
	copy(logRecordByteArray[index+keySize:], logRecord.Value)

	// crc校验和
	crcResult := crc32.ChecksumIEEE(logRecordByteArray[4:])
	binary.LittleEndian.PutUint32(logRecordByteArray[:4], crcResult)

	return logRecordByteArray, size
}

// DecodingLogRecordHeader 反序列化LogRecordHeader
func DecodingLogRecordHeader(buffer []byte) (*LogRecordHeader, int64) {
	// 判断字节大小是否大于4，如果不不大于4表示不足CRC冗余校验和，直接抛出异常即可
	if len(buffer) < 4 {
		return nil, 0
	}

	// varint编码读取第一个字节
	//如果第一个字节为1那么表示还有剩余八个字节可读，
	//如果第一个字节为0，那么表示已经是字节序列末尾了
	//所以varint可以不知道字节长度就能读取字节数组
	keySize, index := binary.Varint(buffer[5:])
	valueSize, _ := binary.Varint(buffer[5+index:])

	logRecordHeader := &LogRecordHeader{
		Crc:       binary.LittleEndian.Uint32(buffer[:4]),
		Type:      buffer[4],
		KeySize:   uint32(keySize),
		ValueSize: uint32(valueSize),
	}
	return logRecordHeader, int64(4 + 1 + 5 + index)
}

// GetLogRecordCRC 获取crc校验和
func GetLogRecordCRC(record *LogRecord, headerBuffer []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBuffer)

	crc32.Update(crc, crc32.IEEETable, record.Key)
	crc32.Update(crc, crc32.IEEETable, record.Value)

	return crc
}
