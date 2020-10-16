package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
	"time"
)

const (
	DIR_FILE_SEPARATOR = byte('/')
)

func init() {
	filer.Stores = append(filer.Stores, &ClickHouseStore{})
}

type ClickHouseStore struct {
	db *sql.DB
}

func (store *ClickHouseStore) GetName() string {
	return "ClickHouse"
}

func (store *ClickHouseStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	dataSource := configuration.GetString(prefix + "url")
	return store.initialize(dataSource)
}

func (store *ClickHouseStore) initialize(dataSource string) (err error) {
	if store.db, err = sql.Open("clickhouse", dataSource); err != nil {
		glog.Errorf("connect to clickhouse %s error: %v", dataSource, err)
		return err
	}
	//test connection
	if err := store.db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			glog.Infof("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			glog.Infoln(err)
		}
	}
	//create table
	//TODO: Use bloom filter to speed up 'like' expression
	if _, err = store.db.Exec(`
		CREATE TABLE IF NOT EXISTS filemeta (
			filename String,
			directory String,
			meta String,
			datetime DateTime
		) engine=ReplacingMergeTree(DateTime) 
		PARTITION BY toYYYYMM(datetime)
		ORDER BY (directory, filename)
	`); err != nil {
		glog.Infof("Clickhouse create filemeta failed, err = %v", err)
	}

	return
}

func (store *ClickHouseStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *ClickHouseStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *ClickHouseStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *ClickHouseStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = weed_util.MaybeGzipData(meta)
	}

	tx, err := store.db.Begin()
	if err != nil {
		return fmt.Errorf("InsertEntry: begin transaction error %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO filemeta (directory,filename,meta, datetime) VALUES(?,?,?,?)")
	if err != nil {
		return fmt.Errorf("InsertEntry error: %s", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(dir, name, meta, time.Now().Format("2006-01-02 15:04:05")); err != nil {
		return fmt.Errorf("InsertEntry error: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("InsertEntry: transaction commit error %s", err)
	}

	return nil
}

func (store *ClickHouseStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *ClickHouseStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()
	var data []byte

	rows, err := store.db.Query("SELECT meta FROM filemeta WHERE directory=? AND filename=?", dir, name)
	if err != nil {
		return nil, fmt.Errorf("Select meta from filemeta where directory = %s and name = %s err : %v",
			dir, name, err)
	}
	defer rows.Close()
	var found bool
	for rows.Next() {
		found = true
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("entry with path %s not found", fullpath)
	}

	glog.Infof("find entry with directory %s and name %s!", dir, name)
	entry = &filer.Entry{
		FullPath: fullpath,
	}
	if err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(data)); err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}
	return entry, nil
}

func (store *ClickHouseStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	if _, err := store.db.Exec(
		"ALTER TABLE filemeta DELETE WHERE directory=? AND filename=?",
		dir, name); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *ClickHouseStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	if _, err := store.db.Exec(
		"ALTER TABLE filemeta DELETE WHERE directory=?", fullpath); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	return nil
}

func (store *ClickHouseStore) ListDirectoryPrefixedEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*filer.Entry, err error) {
	//TODO prefix function not implemented use like 'prefix' grammar
	sqlStr := "SELECT filename, meta FROM filemeta WHERE directory=? AND filename>? ORDER BY filename ASC LIMIT ?"
	if inclusive {
		sqlStr = "SELECT filename, meta FROM filemeta WHERE directory=? AND filename>=? ORDER BY filename ASC LIMIT ?"
	}

	rows, err := store.db.Query(sqlStr, string(fullpath), startFileName, limit)
	if err != nil {
		return nil, fmt.Errorf("select query error :%v", err)
	}

	for rows.Next() {
		var (
			meta []byte
			filename string
		)
		if err := rows.Scan(&filename, &meta); err != nil {
			return nil, fmt.Errorf("list %s : %v", string(fullpath), err)
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(fullpath), filename),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(meta)); decodeErr != nil {
			glog.V(0).Infof("list %s : %v", entry.FullPath, decodeErr)
			break
		}
		entries = append(entries, entry)
	}
	return entries, err
}

func (store *ClickHouseStore) ListDirectoryEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer.Entry, err error) {

	sqlStr := "SELECT filename, meta FROM filemeta WHERE directory=? AND filename>? ORDER BY filename ASC LIMIT ?"
	if inclusive {
		sqlStr = "SELECT filename, meta FROM filemeta WHERE directory=? AND filename>=? ORDER BY filename ASC LIMIT ?"
	}

	rows, err := store.db.Query(sqlStr, string(fullpath), startFileName, limit)
	if err != nil {
		return nil, fmt.Errorf("select query error :%v", err)
	}

	for rows.Next() {
		var (
			meta []byte
			filename string
		)
		if err := rows.Scan(&filename, &meta); err != nil {
			entry := &filer.Entry{
				FullPath: weed_util.NewFullPath(string(fullpath), filename),
			}
			if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(meta)); decodeErr != nil {
				glog.V(0).Infof("list %s : %v", entry.FullPath, decodeErr)
				break
			}
			entries = append(entries, entry)
		}
	}
	return entries, err
}

func (store *ClickHouseStore) Shutdown() {
	//store.db.Exec("drop table filemeta")
	store.db.Close()
}

