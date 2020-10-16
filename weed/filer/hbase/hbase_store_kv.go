package hbase

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/tsuna/gohbase/hrpc"
)

func (store *HbaseStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	values := map[string]map[string][]byte{"cf": map[string][]byte{"value": value}}
	putRequest, err := hrpc.NewPutStr(ctx, "filemeta", string(key), values)
	if err != nil {
		return fmt.Errorf("failed to create put request: %s", err)
	}
	_, err = store.client.Put(putRequest)
	if err != nil {
		return fmt.Errorf("put key %s and value %s error : %v", string(key), string(value), err)
	}
	return nil
}

func (store *HbaseStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	families := map[string][]string{"cf": nil}
	getRequest, err := hrpc.NewGetStr(ctx, "filemeta", string(key), hrpc.Families(families))
	if err != nil {
		return nil, filer.ErrKvNotFound
	}
	getResp, err := store.client.Get(getRequest)
	if err != nil {
		return nil, filer.ErrKvNotFound
	}
	if len(getResp.Cells) == 0 {
		return nil, filer.ErrKvNotFound
	}
	return getResp.Cells[0].Value, nil
}

func (store *HbaseStore) KvDelete(ctx context.Context, key []byte) (err error) {

	deleteRequest, err := hrpc.NewDelStr(ctx, "filemeta", string(key), map[string]map[string][]byte{
		"cf": nil,
	})
	if err != nil {
		return fmt.Errorf("failed to create delete request: %s", err)
	}
	_, err = store.client.Delete(deleteRequest)
	if err != nil {
		return fmt.Errorf("delete key %s error : %v", string(key), err)
	}
	return nil
}

