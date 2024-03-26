package data

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
func EncodingLogRecord(logRecord *LogRecord) ([]byte, *int64) {

	return nil, nil
}

// DecodingLogRecordHeader 反序列化LogRecordHeader
func DecodingLogRecordHeader(buffer []byte) *LogRecordHeader {
	return nil
}
