package hbase

import "github.com/chrislusf/seaweedfs/weed/filer"

func init() {
	filer.Stores = append(filer.Stores, &LevelDBStore{})
}

type HbaseStore struct {

}
