package sqlcache

import (
	"os"
	"sync"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const SqlMetaDatabaseName = string(os.PathSeparator) + "cloud_space_cache.db"

const ErrorOfAlreadyExistsStr = "already exists"
const ErrorOfUniqueKeyExist = "UNIQUE constraint failed"

var globalDbHandle *gorm.DB

var databaseMu sync.RWMutex

func Init(path string) error {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}

	globalDbHandle = db

	err = CreateMetaTable(globalDbHandle)
	if err != nil {
		return err
	}

	err = CreatItemTable(globalDbHandle)
	if err != nil {
		return err
	}

	return nil
}
