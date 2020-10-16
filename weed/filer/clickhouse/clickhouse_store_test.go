package clickhouse

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"testing"
)

func TestCreateAndFind(t *testing.T) {
	testFiler := filer.NewFiler(nil, nil, "", 0, "", "", nil)

	store := &ClickHouseStore{}
	store.initialize("tcp://127.0.0.1:9000?debug=true")
	defer store.Shutdown()
	testFiler.SetStore(store)

	fullpath1 := util.FullPath("/Users/alex/go/src/github.com/AlexanderChiuluvB/seaweedfs/note/seaweedfs.png")
	fullpath2 := util.FullPath("/Users/alex/go/src/github.com/AlexanderChiuluvB/seaweedfs/note/seaweedfs1.png")

	ctx := context.Background()

	defer func() {
		err := testFiler.DeleteEntryMetaAndData(ctx, util.FullPath("/"), true, true, true,
			true, []int32{})
		if err != nil {
			t.Errorf("delete entrymeta and data error :%v", err)
		}
	}()


	entry1 := &filer.Entry{
		FullPath: fullpath1,
		Attr: filer.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	entry2 := &filer.Entry{
		FullPath: fullpath2,
		Attr: filer.Attr{
			Mode: 0440,
			Uid:  1234,
			Gid:  5678,
		},
	}

	if err := testFiler.CreateEntry(ctx, entry1, false, false, nil); err != nil {
		t.Errorf("create entry %v: %v", entry1.FullPath, err)
		return
	}

	if err := testFiler.CreateEntry(ctx, entry2, false, false, nil); err != nil {
		t.Errorf("create entry %v: %v", entry1.FullPath, err)
		return
	}

	findEntry1, err := testFiler.FindEntry(ctx, fullpath1)
	if err != nil {
		t.Errorf("find entry: %v", err)
		return
	}

	findEntry2, err := testFiler.FindEntry(ctx, fullpath2)
	if err != nil {
		t.Errorf("find entry: %v", err)
		return
	}


	if findEntry1.FullPath != entry1.FullPath {
		t.Errorf("find wrong entry: %v", findEntry1.FullPath)
		return
	}

	if findEntry2.FullPath != entry2.FullPath {
		t.Errorf("find wrong entry: %v", findEntry2.FullPath)
		return
	}

	// checking one upper directory
	entries, _ := testFiler.ListDirectoryEntries(ctx, util.FullPath("/Users/alex/go/src/github.com/AlexanderChiuluvB/seaweedfs/note"), "", false, 100, "")
	if len(entries) != 2 {
		t.Errorf("list entries count: %v", len(entries))
		return
	}

}