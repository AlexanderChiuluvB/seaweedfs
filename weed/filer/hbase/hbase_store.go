package hbase

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"io"
	"path"
	"strings"
)

const (
	DIR_FILE_SEPARATOR = byte('/')
)

func init() {
	filer.Stores = append(filer.Stores, &HbaseStore{})
}

type HbaseStore struct {
	client gohbase.Client
	adminClient gohbase.AdminClient
}

func (store *HbaseStore) GetName() string {
	return "hbase"
}

func (store *HbaseStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	return store.initialize(configuration.GetString(prefix + "host"))
}

func (store *HbaseStore) initialize(host string) (err error) {
	store.client = gohbase.NewClient(host)
	store.adminClient = gohbase.NewAdminClient(host)
	cFamilies := map[string]map[string]string{
		"cf": nil,
	}
	createTableRpc := hrpc.NewCreateTable(context.Background(), []byte("filemeta"), cFamilies)
	return store.adminClient.CreateTable(createTableRpc)
}

func (store *HbaseStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *HbaseStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *HbaseStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *HbaseStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.Chunks) > 50 {
		value = weed_util.MaybeGzipData(value)
	}

	values := map[string]map[string][]byte{"cf": map[string][]byte{"value": value}}
	putRequest, err := hrpc.NewPutStr(context.Background(), "filemeta", string(key), values)
	if err != nil {
		return fmt.Errorf("failed to create put request: %s", err)
	}
	_, err = store.client.Put(putRequest)
	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *HbaseStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *HbaseStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())
	families := map[string][]string{"cf": nil}
	getRequest, err := hrpc.NewGetStr(ctx, "filemeta", string(key), hrpc.Families(families))
	if err != nil {
		return nil, fmt.Errorf("failed to create Get request: %s", err)
	}
	getResp, err := store.client.Get(getRequest)
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}
	entry = &filer.Entry{
		FullPath: fullpath,
	}
	if len(getResp.Cells) == 0 {
		return nil, filer_pb.ErrNotFound
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(getResp.Cells[0].Value))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *HbaseStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

	deleteRequest, err := hrpc.NewDelStr(ctx, "filemeta", string(key), map[string]map[string][]byte{
		"cf": nil,
	})
	if err != nil {
		return fmt.Errorf("failed to create delete request: %s", err)
	}
	_, err = store.client.Delete(deleteRequest)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *HbaseStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	var fileName string
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")
	pFilter := filter.NewPrefixFilter([]byte(directoryPrefix))
	filter.NewFirstKeyOnlyFilter()
	scanRequest, err := hrpc.NewScanStr(context.Background(), "filemeta",
		hrpc.Filters(pFilter))
	scanner := store.client.Scan(scanRequest)
	fullPathInStr := string(fullpath)
	for {
		result, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("scan failed withe error %v", err)
		}
		for _, cell := range result.Cells {
			fileName = getNameInCurrentDirectory(cell.Row, fullPathInStr)
			if fileName == "" {
				continue
			}
			deleteRequest, err := hrpc.NewDelStr(ctx, "filemeta", path.Join(string(fullpath), fileName),
				map[string]map[string][]byte{
				"cf": nil,
			})
			if err != nil {
				return fmt.Errorf("failed to create delete request: %s", err)
			}
			_, err = store.client.Delete(deleteRequest)
			if err != nil {
				return fmt.Errorf("delete %s : %v", fullpath, err)
			}
		}
	}
	return nil
}

func (store *HbaseStore) ListDirectoryPrefixedEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*filer.Entry, err error) {
	var fileName string
	var newFullPath weed_util.FullPath
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")
	pFilter := filter.NewPrefixFilter([]byte(directoryPrefix))
	filter.NewFirstKeyOnlyFilter()
	scanRequest, err := hrpc.NewScanStr(context.Background(), "filemeta",
		hrpc.Filters(pFilter))
	scanner := store.client.Scan(scanRequest)
	fullPathInStr := string(fullpath)
	for {
		result, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan failed withe error %v", err)
		}
		for _, cell := range result.Cells {
			fileName = getNameInCurrentDirectory(cell.Row, fullPathInStr)
			if fileName == "" || !strings.HasPrefix(fileName, prefix) {
				continue
			}
			if fileName == startFileName && !inclusive {
				continue
			}
			if fullPathInStr != fileName {
				newFullPath = weed_util.NewFullPath(string(fullpath), fileName)
			} else {
				if !inclusive {
					continue
				}
				newFullPath = weed_util.JoinPath(fileName)
			}
			limit--
			if limit < 0 {
				break
			}
			entry := &filer.Entry{
				FullPath: newFullPath,
			}
			if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(cell.Value)); decodeErr != nil {
				err = decodeErr
				glog.V(0).Infof("list %s : %v", entry.FullPath, err)
				break
			}
			entries = append(entries, entry)
		}
	}
	return entries, err
}

func (store *HbaseStore) ListDirectoryEntries(ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer.Entry, err error) {

	var fileName string
	var newFullPath weed_util.FullPath
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")
	pFilter := filter.NewPrefixFilter([]byte(directoryPrefix))
	filter.NewFirstKeyOnlyFilter()
	scanRequest, err := hrpc.NewScanStr(context.Background(), "filemeta",
		hrpc.Filters(pFilter))
	scanner := store.client.Scan(scanRequest)
	fullPathInStr := string(fullpath)
	for {
		result, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scan failed withe error %v", err)
		}
		for _, cell := range result.Cells {
			fileName = getNameInCurrentDirectory(cell.Row, fullPathInStr)
			if fileName == "" {
				continue
			}
			if fileName == startFileName && !inclusive {
				continue
			}
			if fullPathInStr != fileName {
				newFullPath = weed_util.NewFullPath(string(fullpath), fileName)
			} else {
				newFullPath = weed_util.JoinPath(fileName)
			}
			limit--
			if limit < 0 {
				break
			}
			entry := &filer.Entry{
				FullPath: newFullPath,
			}
			if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(cell.Value)); decodeErr != nil {
				err = decodeErr
				glog.V(0).Infof("list %s : %v", entry.FullPath, err)
				break
			}
			entries = append(entries, entry)
		}
	}
	return entries, err
}

func (store *HbaseStore) Shutdown() {
	store.client.Close()
	disableRpc := hrpc.NewDisableTable(context.Background(), []byte("filemeta"))
	err := store.adminClient.DisableTable(disableRpc)
	if err != nil {
		fmt.Printf("diable table filemeta failed")
	}
	deleteRpc := hrpc.NewDeleteTable(context.Background(), []byte("filemeta"))
	err = store.adminClient.DeleteTable(deleteRpc)
	if err != nil {
		fmt.Printf("delete table filemeta failed")
	}
}

func genKey(dirPath, fileName string) (key []byte) {
	key = []byte(dirPath)
	if dirPath != "/" {
		key = append(key, DIR_FILE_SEPARATOR)
	}
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix []byte) {
	keyPrefix = []byte(string(fullpath))
	if len(startFileName) > 0 {
		if string(fullpath) != "/" {
			keyPrefix = append(keyPrefix, DIR_FILE_SEPARATOR)
		}
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

func getNameInCurrentDirectory(key []byte, directory string) string {
	if string(key) == directory {
		return directory
	}
	fullKey := strings.TrimPrefix(string(key), directory + "/")
	fullKeyLen := len(fullKey)
	idx := 0
	for idx < fullKeyLen {
		if fullKey[idx] == '/' {
			return ""
		}
		idx++
	}
	return fullKey
}