package main

import (
	"errors"
	"fmt"
	"io"
	"kv-database/data"
	"log"
	"os"
)

const (
	MergePath = "/merge/"
)

func (db *Db) Merge() error {
	db.lock.Lock()

	// 判断是否有在合并中 合并只能同时执行一次
	if db.mergeIng {
		return errors.New("正在合并中")
	}
	defer db.lock.Unlock()
	mergeDir := db.option.DirPath + MergePath
	log.Println("合并文件目录:", mergeDir)

	// 将上次合并的缓存文件清除
	os.Remove(mergeDir)
	os.Remove(db.option.DirPath + data.HintFileName)

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
				pos, err = mergeDb.AppendLogRecord(logRecord)
				if err != nil {
					return err
				}
				err = hintFile.WriteHintRecord(logRecord.Key, pos)
				if err != nil {
					return err
				}
			}

			offset += size
		}
	}

	// 5. 整个文件遍历完成后添加一条文件merge完成的记录
	mergerFinishFileIdList = append(mergerFinishFileIdList, mergeDb.activeFile.FileId)
	for fileId := range mergeDb.oldFile {
		mergerFinishFileIdList = append(mergerFinishFileIdList, fileId)

	}

	mergeRecount := &data.MergeFinishRecord{
		FinishCount:         uint32(len(mergeDb.oldFile) + 1),
		MergerFinishFileIds: mergerFinishFileIdList,
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

	mergeDbFile := make([]*data.FileData, 0)
	mergeDbFile = append(mergeDbFile, mergeDb.activeFile)
	err = mergeDb.activeFile.FileManage.Close()
	if err != nil {
		return err
	}

	for oldMergeFileKey := range mergeDb.oldFile {
		oldFile := mergeDb.oldFile[oldMergeFileKey]
		err := oldFile.FileManage.Close()
		if err != nil {
			return err
		}
		mergeDbFile = append(mergeDbFile, oldFile)
	}

	// 删除旧的数据文件
	for fileId := range db.oldFile {
		oldFileData := db.oldFile[fileId]
		err := oldFileData.FileManage.Remove()
		if err != nil {
			return err
		}
	}

	// 移动新的合并文件
	for index := range mergeDbFile {
		mergeFileData := mergeDbFile[index]
		err := os.Rename(mergeDir+fmt.Sprintf("%09d", mergeFileData.FileId)+".data", db.option.DirPath+fmt.Sprintf("%09d", mergeFileData.FileId)+".data")
		if err != nil {
			return err
		}
		fileData, err := data.OpenFileData(db.option.DirPath, mergeFileData.FileId)
		if err != nil {
			return err
		}
		db.oldFile[mergeFileData.FileId] = fileData
	}

	// 重新加载索引信息
	err = db.LoadHintFile(nil)
	if err != nil {
		return err
	}

	return nil
}

func (db Db) getMergePath() string {
	return db.option.DirPath + MergePath
}
