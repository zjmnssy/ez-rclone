package sqlcache

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm/clause"

	"github.com/rclone/rclone/fs"
	"gorm.io/gorm"
)

type ItemMeta struct {
	Path      string `gorm:"column:path" json:"path"`
	Type      int32  `gorm:"column:type" json:"type"`
	Info      []byte `gorm:"column:info" json:"info"`
	CreatedAt int64  `gorm:"autoCreateTime;column:created_at" json:"created_at"`
	UpdatedAt int64  `gorm:"autoUpdateTime;column:updated_at" json:"updated_at"`
}

const SqlMetaTableName = "meta_list"

func CreateMetaTable(db *gorm.DB) error {
	sqlStr := "CREATE TABLE `meta_list` (" +
		"`path` nchar(10240)  NOT NULL PRIMARY KEY," +
		"`type` INT             NOT NULL DEFAULT 0," +
		"`info` BLOB            NULL," +
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

func AddMeta(path string, itype fs.EntryType, info []byte) error {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}

	meta := ItemMeta{
		Path: path,
		Type: int32(itype),
		Info: info,
	}

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlMetaTableName).Create(&meta)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfUniqueKeyExist) {
			return nil
		}

		fs.Errorf(nil, "error = %s", result.Error)
		return result.Error
	}

	return nil
}

func DeleteMeta(path string, itype fs.EntryType) error {
	path = strings.Trim(path, "/")

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlMetaTableName).
		Where("path=? and type=?", path, itype).Delete(ItemMeta{})
	if result.Error != nil {
		fs.Errorf(nil, "error = %s\n", result.Error)
		return result.Error
	}

	return nil
}

func GetMeta(path string, itype fs.EntryType) (ItemMeta, error) {
	path = strings.Trim(path, "/")

	databaseMu.RLock()
	defer databaseMu.RUnlock()

	var meta ItemMeta
	result := globalDbHandle.Table(SqlMetaTableName).
		Where("path=? and type=?", path, itype).
		First(&meta)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			fs.Errorf(nil, "error = %s", result.Error)
		}

		return meta, result.Error
	}

	return meta, nil
}

func UpsertMeta(path string, itype fs.EntryType, info []byte) error {
	path = strings.Trim(path, "/")

	if path == "" {
		return nil
	}

	meta := ItemMeta{
		Type: int32(itype),
		Path: path,
		Info: info,
	}

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlMetaTableName).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "path"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"info": info}),
	}).Create(&meta)
	if result.Error != nil {
		fs.Errorf(nil, "error = %s", result.Error)
		return result.Error
	}

	return nil
}

func RenameItemMeta(oldPath string, newPath string, itype fs.EntryType) error {
	oldPath = strings.Trim(oldPath, "/")
	newPath = strings.Trim(newPath, "/")

	data := make(map[string]interface{}, 8)
	data["path"] = newPath
	data["updated_at"] = time.Now().Unix()

	databaseMu.Lock()
	defer databaseMu.Unlock()

	result := globalDbHandle.Table(SqlMetaTableName).
		Where("path=? and type=?", oldPath, itype).Updates(data)
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), ErrorOfUniqueKeyExist) {
			err := globalDbHandle.Transaction(func(tx *gorm.DB) error {
				var sqlMeta ItemMeta
				rd := tx.Table(SqlMetaTableName).
					Where("path=? and type=?", newPath, itype).
					Delete(&sqlMeta)
				if rd.Error != nil {
					fs.Errorf(nil, "error = %s", result.Error)
					return rd.Error
				}

				ru := tx.Table(SqlMetaTableName).
					Where("path=? and type=?", oldPath, itype).
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

func RenameDirMeta(oldPath string, newPath string) error {
	oldPath = strings.Trim(oldPath, "/")
	newPath = strings.Trim(newPath, "/")

	databaseMu.Lock()
	defer databaseMu.Unlock()

	txErr := globalDbHandle.Transaction(func(tx *gorm.DB) error {
		dataSelf := make(map[string]interface{}, 8)
		dataSelf["path"] = newPath
		dataSelf["updated_at"] = time.Now().Unix()

		resultSelf := tx.Table(SqlMetaTableName).
			Where("path=? and type=?", oldPath, fs.EntryDirectory).
			Updates(dataSelf)
		if resultSelf.Error != nil {
			fs.Errorf(nil, "error = %s", resultSelf.Error)
			return resultSelf.Error
		}

		dataLeaf := make(map[string]interface{}, 8)
		dataLeaf["path"] = gorm.Expr(fmt.Sprintf("replace(path, '%s/', '%s/')", oldPath, newPath))
		dataLeaf["updated_at"] = time.Now().Unix()

		resultLeaf := tx.Table(SqlMetaTableName).
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

func CleanAllCache() error {
	databaseMu.Lock()
	defer databaseMu.Unlock()

	txErr := globalDbHandle.Transaction(func(tx *gorm.DB) error {
		resultMeta := tx.Exec(fmt.Sprintf("delete from %s;", SqlMetaTableName))
		if resultMeta.Error != nil {
			fs.Errorf(nil, "error = %s", resultMeta.Error)
			return resultMeta.Error
		}

		resultItem := tx.Exec(fmt.Sprintf("delete from %s;", SqlItemTableName))
		if resultItem.Error != nil {
			fs.Errorf(nil, "error = %s", resultItem.Error)
			return resultItem.Error
		}

		return nil
	})
	if txErr != nil {
		fs.Errorf(nil, "error = %s", txErr)
		return txErr
	}

	return nil
}

func GetAllMeta() ([]ItemMeta, error) {
	list := make([]ItemMeta, 0, 1000)

	databaseMu.RLock()
	defer databaseMu.RUnlock()

	result := globalDbHandle.Table(SqlMetaTableName).Find(&list)
	if result.Error != nil {
		fs.Errorf(nil, "error = %s", result.Error)
		return list, result.Error
	}

	return list, nil
}
