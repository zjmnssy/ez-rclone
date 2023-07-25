package sqlcache

import (
	"fmt"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"gorm.io/gorm"
)

type Item struct {
	Path      string `gorm:"column:path" json:"path"`
	Type      int32  `gorm:"column:type" json:"type"`
	CreatedAt int64  `gorm:"autoCreateTime;column:created_at" json:"created_at"`
	UpdatedAt int64  `gorm:"autoUpdateTime;column:updated_at" json:"updated_at"`
}

const SqlItemTableName = "item_list"

func CreatItemTable(db *gorm.DB) error {
	sqlStr := "CREATE TABLE `item_list` (" +
		"`path` nchar(10240)   NOT NULL PRIMARY KEY," +
		"`type` INT            NOT NULL DEFAULT 0," +
		"`created_at` INT       NOT NULL DEFAULT 0," +
		"`updated_at` INT       NOT NULL DEFAULT 0" +
		");"
	result := db.Exec(sqlStr)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfAlreadyExistsStr) {
			return nil
		}

		fs.Errorf(nil, "create table meta_list error = %s", result.Error)
		return result.Error
	}

	return nil
}

func AddItem(path string, itype fs.EntryType) error {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}

	meta := Item{
		Path: path,
		Type: int32(itype),
	}

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlItemTableName).Create(&meta)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfUniqueKeyExist) {
			return nil
		}

		fs.Errorf(nil, "error = %s", result.Error)
		return result.Error
	}

	return nil
}

func DeleteItem(path string, itype fs.EntryType) error {
	path = strings.Trim(path, "/")

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlItemTableName).
		Where("path=? and type=?", path, itype).Delete(Item{})
	if result.Error != nil {
		fs.Errorf(nil, "error = %s\n", result.Error)
		return result.Error
	}

	return nil
}

func RenameItem(oldPath string, newPath string) error {
	oldPath = strings.Trim(oldPath, "/")
	newPath = strings.Trim(newPath, "/")

	data := make(map[string]interface{}, 8)
	data["path"] = newPath
	data["updated_at"] = time.Now().Unix()

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlItemTableName).
		Where("path=? and type=?", oldPath, fs.EntryObject).Updates(data)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfUniqueKeyExist) {
			err := globalDbHandle.Transaction(func(tx *gorm.DB) error {
				var sqlMeta Item
				rd := tx.Table(SqlItemTableName).
					Where("path=? and type=?", newPath, fs.EntryObject).
					Delete(&sqlMeta)
				if rd.Error != nil {
					fs.Errorf(nil, "error = %s", result.Error)
					return rd.Error
				}

				ru := tx.Table(SqlItemTableName).
					Where("path=? and type=?", oldPath, fs.EntryObject).
					Updates(data)
				if ru.Error != nil {
					fs.Errorf(nil, "error = %s", result.Error)
					return rd.Error
				}

				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}

		fs.Errorf(nil, "error = %s\n", result.Error)
		return result.Error
	}

	return nil
}

func RenameDirItem(oldPath string, newPath string) error {
	oldPath = strings.Trim(oldPath, "/")
	newPath = strings.Trim(newPath, "/")

	databaseMu.Lock()
	defer databaseMu.Unlock()

	txErr := globalDbHandle.Transaction(func(tx *gorm.DB) error {
		dataSelf := make(map[string]interface{}, 8)
		dataSelf["path"] = newPath
		dataSelf["updated_at"] = time.Now().Unix()

		resultSelf := tx.Table(SqlItemTableName).
			Where("path=? and type=?", oldPath, fs.EntryDirectory).
			Updates(dataSelf)
		if resultSelf.Error != nil {
			fs.Errorf(nil, "error = %s", resultSelf.Error)
			return resultSelf.Error
		}

		dataLeaf := make(map[string]interface{}, 8)
		dataLeaf["path"] = gorm.Expr(fmt.Sprintf("replace(path, '%s/', '%s/')", oldPath, newPath))
		dataLeaf["updated_at"] = time.Now().Unix()

		resultLeaf := tx.Table(SqlItemTableName).
			Where("path like ?", strings.Trim(oldPath, "/")+"%").
			Updates(dataLeaf)
		if resultLeaf.Error != nil {
			fs.Errorf(nil, "error = %s", resultLeaf.Error)
			return resultLeaf.Error
		}

		return nil
	})
	if txErr != nil {
		fs.Errorf(nil, "error = %s", txErr)
		return txErr
	}

	return nil
}

func GetAllItem() ([]Item, error) {
	list := make([]Item, 0, 1000)

	databaseMu.RLock()
	defer databaseMu.RUnlock()

	result := globalDbHandle.Table(SqlItemTableName).Find(&list)
	if result.Error != nil {
		fs.Errorf(nil, "error = %s", result.Error)
		return list, result.Error
	}

	return list, nil
}
