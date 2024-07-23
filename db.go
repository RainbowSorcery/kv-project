package main

import (
	"errors"
	"io"
	"kv-database/data"
	"kv-database/index"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	// 全局事务编号
	TranNum *int64
	// 是否merge中
	mergeIng bool
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
		TranNum:    new(int64),
	}

	// 初始化db
	err := db.LoadDb()
	if err != nil {
		return nil, err
	}

	// 读取活动文件 并记录上次写文件的位置
	offset, err := readFileData(db, db.activeFile)
	if err != nil && err == io.EOF {
		log.Printf("文件读取完毕, fileId:%d\n", db.activeFile.FileId)
	} else if err != nil && err != io.EOF {
		return nil, err
	}

	db.activeFile.WriteOffset = offset

	// 读取非活动文件
	for _, oldFileData := range db.oldFile {
		_, err = readFileData(db, oldFileData)
		if err != nil && err == io.EOF {
			log.Printf("文件读取完毕, fileId:%d\n", oldFileData.FileId)
		} else if err != nil && err != io.EOF {
			return nil, err
		}
	}

	return db, nil
}

func readFileData(db *Db, activeFile *data.FileData) (int64, error) {
	// 事务暂存数据
	txCache := make(map[int64]map[string]*data.LogRecordPos)

	var offset int64 = 0
	for {
		logRecord, size, err := activeFile.Read(offset)

		// 根据偏移读取文件内容 如果文件内容EOF了，那么表示文件读取完毕了
		if err != nil && err == io.EOF {
			break
		}

		txNum, key := DecodingTranKey(logRecord.Key)
		// 判断record状态 如果是事务提交对象则暂存到缓存区中 如果不是则判断元素是否被删除 如果被删除则从内存索引中将元素移除
		if txNum != 0 && logRecord.Type != data.TxComplete {
			txValueMap := txCache[txNum]
			if txValueMap == nil {
				txValueMap = make(map[string]*data.LogRecordPos)
			}
			if logRecord.Type == data.Normal {
				txValueMap[string(key)] = &data.LogRecordPos{
					FileId: activeFile.FileId,
					Pos:    offset,
				}
			} else if logRecord.Type == data.Deleted {
				txValueMap[string(key)] = nil
			}

			txCache[txNum] = txValueMap
		} else if logRecord.Type == data.Normal {
			db.index.Put(key, &data.LogRecordPos{
				FileId: activeFile.FileId,
				Pos:    offset,
			})
		} else if logRecord.Type == data.Deleted {
			db.index.Delete(key)
		} else if logRecord.Type == data.TxComplete {
			// 如果遇到事务索引以完成则读取事务数据到内存中
			txKey, _ := DecodingTranKey(logRecord.Key)
			txValue := txCache[txKey]
			for key := range txValue {
				if txValue[key] != nil {
					db.index.Put([]byte(key), txValue[key])
				} else {
					db.index.Delete([]byte(key))
				}
			}
			delete(txCache, txNum)
			db.TranNum = &txNum
			//db.TranNum = &txNum
		}

		// 计算下个record偏移
		offset += size
	}
	return offset, nil
}

// LoadDb 加载db文件
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

	// 如果目录下没有文件 那么初始化一个活动文件
	if fileDataArr == nil || len(fileDataArr) == 0 {
		fileData, err := data.OpenFileData(db.option.DirPath, uint32(0))
		if err != nil {
			return err
		}
		db.activeFile = fileData
	} else {
		// 对目录下的文件进行排序
		sort.Slice(fileDataArr, func(i, j int) bool {
			return fileDataArr[i].Name() < fileDataArr[j].Name()
		})

		for i, file := range fileDataArr {
			fileExt := filepath.Ext(file.Name())
			if fileExt == ".data" && !file.IsDir() {
				fileId, err := strconv.ParseUint(strings.ReplaceAll(file.Name(), ".data", ""), 10, 32)
				if err != nil {
					return err
				}
				fileData, err := data.OpenFileData(db.option.DirPath, uint32(fileId))
				if err != nil {
					return err
				}

				// id最大的代表该文件为活跃文件 文件索引从0开始 所以需要 - 1
				if i == len(fileDataArr)-1 {
					db.activeFile = fileData
				} else {
					db.oldFile[uint32(fileId)] = fileData
				}
			}
		}
	}

	return nil
}

// Put 添加kv
func (db *Db) Put(key []byte, value []byte) error {
	// 判断key是否合法
	if len(key) == 0 {
		return errors.New("key为空")
	}

	// 构建logRecord
	logRecord := &data.LogRecord{
		Key:   EncodingTranKey(key, 0),
		Value: value,
		Type:  data.Normal,
	}

	// 向文件追加数据
	logRecordPos, err := db.appendLogRecordSync(logRecord)
	if err != nil {
		return errors.New("文件追加失败")
	}

	// 将追加的索引添加内存中
	db.index.Put(key, logRecordPos)

	return nil
}

// Delete 删除kv
func (db *Db) Delete(key []byte) error {
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
		Key:  EncodingTranKey(key, 0),
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

func (db *Db) appendLogRecordSync(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendLogRecord(logRecord)
}

// 将KV数据追加到文件中
func (db *Db) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 如果当前活跃文件为空 则创建当前活跃文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 判断文件是否到达阈值 如果到达阈值则将旧的数据文件归档，创建新的数据文件
	if db.activeFile.WriteOffset >= db.option.FileDataSize {
		db.oldFile[db.activeFile.FileId] = db.activeFile
		err := db.Close()
		if err != nil {
			return nil, err
		}

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

// 设置活动文件
func (db *Db) setActiveFile() error {
	activeFileId := db.activeFile.FileId
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
	record, err := db.posByLogRecord(keyIndex)
	if err != nil {
		return nil, err
	}

	if record.Type == data.Deleted {
		return nil, errors.New("key已删除")
	}

	return record, nil
}

func (db *Db) posByLogRecord(pos *data.LogRecordPos) (*data.LogRecord, error) {

	if pos == nil {
		return nil, errors.New("索引不存在")
	}

	var fileData *data.FileData

	// 判断文件是否为活跃文件
	if pos.FileId == db.activeFile.FileId {
		fileData = db.activeFile
	} else {
		// 从old file中获取文件数据
		fileData = db.oldFile[pos.FileId]
	}

	// 判断文件是否存在
	if fileData == nil {
		return nil, errors.New("文件不存在")
	}

	record, err := fileData.ReadLogRecord(pos.Pos)
	if err != nil {
		return nil, err
	}

	if record == nil {
		return nil, errors.New("log record不存在")
	}

	return record, nil
}

func (db *Db) KeyList() [][]byte {

	return nil
}

// Sync 将缓冲区的数据持久化到内存中
func (db *Db) Sync() error {
	err := db.activeFile.FileManage.Sync()
	return err
}

// Close 关闭文件读写
func (db *Db) Close() error {
	return db.activeFile.FileManage.Close()
}

func (db *Db) ListKeys() ([][]byte, error) {
	iterate := db.index.Iterate(false)
	keys := make([][]byte, db.index.Size())

	keyIndex := 0
	for !iterate.HasNext() {
		key, err := iterate.Key()
		if err != nil {
			return nil, err
		}
		keys[keyIndex] = key
		keyIndex++
		iterate.Next()
	}

	return keys, nil
}

func (db *Db) fold(fun func(key []byte, value []byte) bool) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	iterate := db.index.Iterate(false)

	// 判断迭代器是否还有key
	for !iterate.HasNext() {
		key, err := iterate.Key()

		if err != nil {
			return err
		}

		pos, err := iterate.Value()
		if err != nil {
			return err
		}
		value, err := db.posByLogRecord(pos)
		if err != nil {
			return err
		}
		if !fun(key, value.Value) {
			break
		}

		iterate.Next()
	}

	return nil
}
