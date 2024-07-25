package main

import (
	"errors"
	"io"
	"kv-database/data"
	"log"
	"os"
)

const (
	MergePath      = "/merge/"
	FinishMergeKey = "FinishMergeKey"
)

type MergeFinishRecord struct {
	// 合并成功数
	FinishCount uint32
	// 合并成功文件id列表
	mergerFinishFileIds []uint32
}

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

	// 清空上次合并的文件信息
	err = os.RemoveAll(mergePath)
	if err != nil {
		return err
	}

	// 1. 获取所有非活动文件 也就是需要merge的文件
	oldFileMap := db.oldFile

	// 创建合并实例
	mergeDb, err := open(option{
		DirPath:      mergePath,
		FileDataSize: 1024,
	})

	// 4. 打开hint文件
	hintFile, err := data.OpenHintFile(db.option.DirPath)

	var mergerFinishFileIdList []uint32

	// 2. 遍历文件中的LogRecord
	for oldFileKey := range oldFileMap {
		oldFile := oldFileMap[oldFileKey]

		if err != nil {
			return err
		}

		var offset int64 = 0
		for {
			logRecord, size, err := oldFile.Read(offset)
			if err == io.EOF {
				break
			}

			if logRecord == nil {
				return errors.New("logRecord解析错误")
			}

			_, realKey := DecodingTranKey(logRecord.Key)
			pos := db.index.Get(realKey)

			// 3. 判断LogRecord中的数据是否与内存索引一致 如果一致则新建hint文件
			if pos != nil && pos.Pos == offset && pos.FileId == oldFile.FileId {
				_, err = mergeDb.AppendLogRecord(logRecord)
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
		mergerFinishFileIdList = append(mergerFinishFileIdList, oldFileKey)
	}

	mergeRecount := &MergeFinishRecord{
		FinishCount:         uint32(len(oldFileMap)),
		mergerFinishFileIds: mergerFinishFileIdList,
	}

	// 将合并完成记录写入到文件中
	mergeFinsFile, err := data.OpenFinishMergeFile(db.option.DirPath)
	if err != nil {
		return err
	}

	err = mergeFinsFile.WriteMergeFinishRecord(mergeRecount)

	if err != nil {
		return err
	}

	return nil
}

func (db Db) getMergePath() string {
	return db.option.DirPath + MergePath
}
