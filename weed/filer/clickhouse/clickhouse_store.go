package clickhouse

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
	"github.com/ClickHouse/clickhouse-go"
	"database/sql"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
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
	dataSource := configuration.GetString(prefix + "dataSource")
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
	key := genKey(entry.DirAndName())

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.Chunks) > 50 {
		value = weed_util.MaybeGzipData(value)
	}

	err = store.db.Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.Chunks))

	return nil
}

func (store *ClickHouseStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *ClickHouseStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	data, err := store.db.Get(key, nil)

	if err == leveldb.ErrNotFound {
		return nil, filer_pb.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", entry.FullPath, err)
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData((data)))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	// println("read", entry.FullPath, "chunks", len(entry.Chunks), "data", len(data), string(data))

	return entry, nil
}

func (store *ClickHouseStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

	err = store.db.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *ClickHouseStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {

	batch := new(leveldb.Batch)

	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")
	iter := store.db.NewIterator(&leveldb_util.Range{Start: directoryPrefix}, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		batch.Delete([]byte(genKey(string(fullpath), fileName)))
	}
	iter.Release()

	err = store.db.Write(batch, nil)

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *ClickHouseStore) ListDirectoryPrefixedEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*filer.Entry, err error) {
	return nil, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *ClickHouseStore) ListDirectoryEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer.Entry, err error) {

	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	iter := store.db.NewIterator(&leveldb_util.Range{Start: genDirectoryKeyPrefix(fullpath, startFileName)}, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		if fileName == startFileName && !inclusive {
			continue
		}
		limit--
		if limit < 0 {
			break
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(fullpath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(iter.Value())); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
	}
	iter.Release()

	return entries, err
}

func genKey(dirPath, fileName string) (key []byte) {
	key = []byte(dirPath)
	key = append(key, DIR_FILE_SEPARATOR)
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix []byte) {
	keyPrefix = []byte(string(fullpath))
	keyPrefix = append(keyPrefix, DIR_FILE_SEPARATOR)
	if len(startFileName) > 0 {
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix
}

func getNameFromKey(key []byte) string {

	sepIndex := len(key) - 1
	for sepIndex >= 0 && key[sepIndex] != DIR_FILE_SEPARATOR {
		sepIndex--
	}

	return string(key[sepIndex+1:])

}

func (store *ClickHouseStore) Shutdown() {
	store.db.Close()
}

