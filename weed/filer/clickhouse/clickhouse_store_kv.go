package clickhouse

import "context"

func (store *ClickHouseStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	panic("implement me")
}

func (store *ClickHouseStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	panic("implement me")
}

func (store *ClickHouseStore) KvDelete(ctx context.Context, key []byte) (err error) {
	panic("implement me")
}
