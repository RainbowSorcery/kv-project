package kv_database

import (
	"errors"
	"io"
	"kv-database/data"
	"kv-database/index"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// Db bitcask实例 面向用户的接口
type Db struct {
	// 系统配置
	option option
	// 锁
	lock *sync.RWMutex
	// 活动文件
	activeFile *data.FileData
	// 老文件列表 只允许读 不允许写
	oldFile map[uint32]*data.FileData
	// 内存中存储的索引信息
	index index.Indexer
}

func open(option option) (*Db, error) {
	// 校验option配置是否合法
	if len(option.DirPath) == 0 {
		return nil, errors.New("目录为空")
	}

	db := &Db{
		option:     option,
		lock:       new(sync.RWMutex),
		activeFile: nil,
		oldFile:    make(map[uint32]*data.FileData, 10),
		index:      index.NewBtree(),
	}

	err := db.LoadDb()
	if err != nil {
		return nil, err
	}

	// 读取活动文件 并记录上次写文件的位置
	offset := readFileData(db, db.activeFile)
	db.activeFile.WriteOffset = offset

	// 读取非活动文件
	for _, oldFileData := range db.oldFile {
		readFileData(db, oldFileData)
	}

	return db, nil
}

func readFileData(db *Db, activeFile *data.FileData) int64 {
	// 根据偏移读取我文件内容 如果文件内容EOF了，那么表示文件读取完毕了
	var offset int64 = 0
	for {
		logRecord, size, err := activeFile.Read(offset)
		if err == io.EOF {
			break
		}

		// 如果logRecord是未被删除的 那么加入到索引内存中 否则删除
		if logRecord.Type == data.Normal {
			db.index.Put(logRecord.Key, &data.LogRecordPos{
				FileId: db.activeFile.FileId,
				Pos:    offset,
			})
		} else {
			db.index.Delete(logRecord.Key)
		}
		// 计算下个record偏移
		offset += size
	}
	return offset
}

func (db *Db) LoadDb() error {

	// 判断目录是否存在 如果不存在则创建
	_, err := os.Stat(db.option.DirPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(db.option.DirPath, 0644)
		if err != nil {
			return err
		}
	}
	// 获取目录下所有以data后缀的文件
	fileDataArr, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}
	// 对目录下的文件进行排序
	sort.Slice(fileDataArr, func(i, j int) bool {
		return fileDataArr[i].Name() < fileDataArr[j].Name()
	})

	for i, file := range fileDataArr {
		fileExt := filepath.Ext(file.Name())
		if fileExt == "data" && !file.IsDir() {
			fileId, err := strconv.ParseUint(file.Name(), 10, 32)
			if err != nil {
				return err
			}
			fileData, err := data.OpenFileData(db.option.DirPath, uint32(fileId))
			if err != nil {
				return err
			}

			if i == len(fileDataArr) {
				db.activeFile = fileData
			} else {
				db.oldFile[uint32(fileId)] = fileData
			}
		}
	}
	return nil
}

func (db *Db) Put(key []byte, value []byte) error {
	// 判断key是否合法
	if len(key) == 0 {
		return errors.New("key为空")
	}

	// 构建logRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.Normal,
	}

	// 向文件追加数据
	logRecordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return errors.New("文件追加失败")
	}

	// 将追加的索引添加内存中
	db.index.Put(key, logRecordPos)

	return nil
}

func (db *Db) delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 校验key是否合法
	if len(key) == 0 {
		return errors.New("key为空")
	}

	// 判断key是否在内存中存在
	if db.index.Get(key) == nil {
		return errors.New("key不存在")
	}

	// 新建一个LogRecord并写入到磁盘中 在合并时再将墓碑值修改
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.Deleted,
	}

	_, err := db.appendLogRecord(logRecord)

	if err != nil {
		return err
	}

	// 删除内存中的索引
	db.index.Delete(key)

	return nil
}

// 将KV数据追加到文件中
func (db *Db) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 如果当前活跃文件为空 则创建当前活跃文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 判断文件是否到达阈值 如果到达阈值则将旧的数据文件归档，创建新的数据文件
	if db.activeFile.WriteOffset >= db.option.FileDataSize {
		db.oldFile[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 将记录对象序列化为二进制字节数组
	encodingData, _ := data.EncodingLogRecord(logRecord)

	offset := db.activeFile.WriteOffset

	// 写入到文件中
	err := db.activeFile.Write(encodingData)
	if err != nil {
		return nil, err
	}

	return &data.LogRecordPos{
		FileId: db.activeFile.FileId,
		Pos:    offset,
	}, nil
}

func (db *Db) setActiveFile() error {
	activeFileId := 0
	// 如果oldFile存在的话那么activeFile的id等于oldFile的id + 1 如果oldFile不存在的话ActiveFile的id就是0
	if db.activeFile != nil {
		activeFileId += 1
	}

	fileData, openFileDataError := data.OpenFileData(db.option.DirPath, uint32(activeFileId))
	if openFileDataError != nil {
		return errors.New("创建数据文件失败")
	}

	db.activeFile = fileData

	return nil
}

func (db *Db) read(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key为空")
	}

	keyIndex := db.index.Get(key)
	if keyIndex == nil {
		return nil, errors.New("key不存在")
	}

	// 判断活动文件是否与index的file id相符
	var fileData *data.FileData

	if db.activeFile.FileId == keyIndex.FileId {
		fileData = db.activeFile
	} else {
		fileData = db.oldFile[keyIndex.FileId]
	}

	if fileData == nil {
		return nil, errors.New("文件索引不存在")
	}

	logRecord, _, err := fileData.Read(keyIndex.Pos)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.Deleted {
		return nil, nil
	} else {
		return logRecord.Value, nil
	}
}

// Get 根据key获取logRecord
func (db *Db) Get(key []byte) (*data.LogRecord, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// 在内存中查找key是否存在 如果不存在则直接抛出异常
	keyIndex := db.index.Get(key)

	if keyIndex == nil {
		return nil, errors.New("索引不存在")
	}

	var fileData *data.FileData

	// 判断文件是否为活跃文件
	if keyIndex.FileId == db.activeFile.FileId {
		fileData = db.activeFile
	} else {
		// 从old file中获取文件数据
		fileData = db.oldFile[keyIndex.FileId]
	}

	// 判断文件是否存在
	if fileData == nil {
		return nil, errors.New("文件不存在")
	}

	record, err := fileData.ReadLogRecord(keyIndex.Pos)
	if err != nil {
		return nil, err
	}

	if record == nil {
		return nil, errors.New("log record不存在")
	}

	if record.Type == data.Deleted {
		return nil, errors.New("key已删除")
	}

	return record, nil
}
