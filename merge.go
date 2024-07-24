package main

import (
	"errors"
	"io"
	"kv-database/data"
	"log"
	"os"
)

const (
	MergePath = "/merge"
)

func (db *Db) Merge() error {
	db.lock.Lock()

	// 判断是否有在合并中 合并只能同时执行一次
	if db.mergeIng {
		return errors.New("正在合并中")
	}
	defer db.lock.Unlock()
	log.Println(db.option.DirPath + MergePath)

	// 判断合并文件是否存在
	mergePath := db.getMergePath()
	_, err := os.Stat(mergePath)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(mergePath, 0644)
		if err != nil {
			return err
		}
	}

	// 1. 获取所有非活动文件 也就是需要merge的文件
	oldFileMap := db.oldFile

	// 创建合并实例
	mergeDb, err := open(option{
		DirPath:      mergePath,
		FileDataSize: 1024,
	})

	// 4. 打开hint文件
	hintFile, err := data.OpenHintFile(mergePath)

	// 2. 遍历文件中的LogRecord
	for oldFileKey := range oldFileMap {
		oldFile := oldFileMap[oldFileKey]

		if err != nil {
			return err
		}

		for {
			var offset int64 = 0
			logRecord, size, err := oldFile.Read(offset)
			if err == io.EOF {
				break
			}

			if logRecord == nil {
				return errors.New("logRecord解析错误")
			}

			pos := db.index.Get(logRecord.Key)

			// 3. 判断LogRecord中的数据是否与内存索引一致 如果一致则新建hint文件
			if pos != nil && pos.Pos == offset && pos.FileId == oldFile.FileId {
				_, err = mergeDb.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
			}

			err = hintFile.WriteHintRecord(pos)
			if err != nil {
				return err
			}
			offset += size
		}
		// 5. 整个文件遍历完成后添加一条文件merge完成的记录
	}

	return nil
}

func (db *Db) getMergePath() string {
	return db.option.DirPath + MergePath
}