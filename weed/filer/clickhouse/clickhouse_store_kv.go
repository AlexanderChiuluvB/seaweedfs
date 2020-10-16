package clickhouse

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"time"
)

func (store *ClickHouseStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dir, name := genDirAndName(key)
	tx, err := store.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction error %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO filemeta (directory, filename, meta, datetime) VALUES(?,?,?,?)")
	if err != nil {
		return fmt.Errorf("kv insert: %s", err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(dir, name, value, time.Now().Format("2006-01-02 15:04:05")); err != nil {
		return fmt.Errorf("kv insert: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("kv insert: transaction commit error %s", err)
	}

	return nil
}

func (store *ClickHouseStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dir, name := genDirAndName(key)

	result, err := store.db.Query(
		"SELECT meta FROM filemeta WHERE directory=? AND filename=?", dir, name)
	if err != nil {
		return nil, filer.ErrKvNotFound
	}
	for result.Next() {
		if err = result.Scan(&value); err != nil {
			return nil, filer.ErrKvNotFound
		}
	}

	if len(value) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return value, nil
}

func (store *ClickHouseStore) KvDelete(ctx context.Context, key []byte) (err error) {
	dir, name := genDirAndName(key)

	if _, err := store.db.Query(
		"ALTER TABLE filemeta DELETE WHERE directory=? AND filename=?",
		dir, name); err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}

func genDirAndName(key []byte) (dir string, name string) {
	for len(key) < 8 {
		key = append(key, 0)
	}

	dir = string(key[:8])
	name = string(key[8:])

	return
}