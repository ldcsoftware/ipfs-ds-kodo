package kodods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ldcsoftware/qiniu-go-sdk/api.v8/kodo"
	"github.com/ldcsoftware/qiniu-go-sdk/syncdata/operation"
	"github.com/stretchr/testify/assert"
)

type mockHttpCode struct {
	code int
}

func (m *mockHttpCode) HttpCode() int {
	return m.code
}

func (m *mockHttpCode) Error() string {
	if m.code == 0 {
		return ""
	}
	return "mockErr:" + fmt.Sprint(m.code)
}

func errorOf(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}

func httpCodeOf(err error) int {
	if err == nil {
		return 200
	}
	var hc httpCoder
	if !errors.As(err, &hc) {
		return 599
	} else {
		return hc.HttpCode()
	}
}

type mockKodoApi struct {
	files    map[string][]byte
	errorDel map[string]struct{}
	mutex    sync.RWMutex
}

func (m *mockKodoApi) getSortKeys() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	keys := make([]string, 0, len(m.files))
	for key := range m.files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (m *mockKodoApi) UploadData(ctx context.Context, key string, data []byte, ret interface{}) (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.files[key] = data
	return nil
}

func (m *mockKodoApi) fileCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.files)
}

func (m *mockKodoApi) DownloadBytes(key string) (data []byte, err error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	data, ok := m.files[key]
	if !ok {
		return nil, &mockHttpCode{612}
	}
	return data, nil
}

func (m *mockKodoApi) Stat(ctx context.Context, key string) (entry kodo.Entry, err error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	data, ok := m.files[key]
	if !ok {
		return entry, &mockHttpCode{612}
	}
	entry.Fsize = int64(len(data))
	return entry, nil
}

func (m *mockKodoApi) Delete(ctx context.Context, key string) (err error) {
	_, ok := m.errorDel[key]
	if ok {
		return fmt.Errorf("test delete error, key:%v", key)
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, ok = m.files[key]
	if !ok {
		return &mockHttpCode{612}
	}
	delete(m.files, key)
	return nil
}

func (m *mockKodoApi) BatchDelete(ctx context.Context, keys ...string) (rets []kodo.BatchItemRet, err error) {
	for _, key := range keys {
		err2 := m.Delete(ctx, key)
		rets = append(rets, kodo.BatchItemRet{
			Error: errorOf(err2),
			Code:  httpCodeOf(err2),
		})
	}
	return rets, nil
}

func (m *mockKodoApi) ListPrefix(ctx context.Context, prefix, marker string, limit int) (entrys []kodo.ListItem, markerOut string, err error) {
	idx, err := strconv.Atoi(marker)
	if err != nil {
		return entrys, "", err
	}
	if idx >= m.fileCount() {
		return entrys, "", io.EOF
	}

	keys := m.getSortKeys()
	for ; idx < len(keys); idx++ {
		if limit <= 0 {
			return entrys, fmt.Sprint(idx), nil
		}
		if strings.HasPrefix(keys[idx], prefix) {
			limit--
			entry, err := m.Stat(ctx, keys[idx])
			if err != nil {
				return entrys, "", err
			}
			entrys = append(entrys, kodo.ListItem{
				Key:   keys[idx],
				Fsize: entry.Fsize,
			})
		}
	}
	return entrys, "", io.EOF
}

func TestKodoDsBaseApi(t *testing.T) {
	cfg := &Config{
		Config: &operation.Config{
			UcHosts: []string{"test"},
		},
		DeleteLimit:      3,
		BatchConcurrency: 4,
	}
	cfg.fixConfig()

	kodoApi := &mockKodoApi{
		files:    make(map[string][]byte),
		errorDel: make(map[string]struct{}),
	}

	obs := &KodoDs{
		Config:     cfg,
		uploader:   kodoApi,
		downloader: kodoApi,
		lister:     kodoApi,
	}

	key := ds.NewKey("key1")
	err := obs.Delete(key)
	assert.NoError(t, err)

	size, err := obs.GetSize(key)
	assert.Error(t, ds.ErrNotFound, err)
	assert.Equal(t, -1, size)

	exists, err := obs.Has(key)
	assert.NoError(t, err)
	assert.False(t, exists)

	val := []byte("123")
	err = obs.Put(key, val)
	assert.NoError(t, err)
	size, err = obs.GetSize(key)
	assert.NoError(t, err)
	assert.Equal(t, len(val), size)
}

func TestKodoDsQuery(t *testing.T) {

}

func TestBatchOp(t *testing.T) {
	Prefix := "ldc"
	KeyPrefix := "key"
	cfg := &Config{
		Config: &operation.Config{
			UcHosts: []string{"test"},
		},
		Prefix:           Prefix,
		DeleteLimit:      3,
		BatchConcurrency: 4,
	}
	cfg.fixConfig()

	kodoApi := &mockKodoApi{
		files:    make(map[string][]byte),
		errorDel: make(map[string]struct{}),
	}

	obs := &KodoDs{
		Config:     cfg,
		uploader:   kodoApi,
		downloader: kodoApi,
		lister:     kodoApi,
	}
	batch, err := obs.Batch()
	assert.NoError(t, err)

	keyCount := 10
	keyStart := 0
	for i := 0; i < keyCount; i++ {
		key := KeyPrefix + fmt.Sprint(i+1+keyStart)
		dsKey := ds.NewKey(key)
		err = batch.Put(dsKey, []byte(key))
		assert.NoError(t, err)
	}
	assert.Equal(t, 0, len(kodoApi.files))
	batch.Commit()
	assert.Equal(t, keyCount, len(kodoApi.files))

	batch, err = obs.Batch()
	assert.NoError(t, err)
	keyStart += keyCount

	for i := 0; i < keyCount; i++ {
		key := KeyPrefix + fmt.Sprint(i+1+keyStart)
		dsKey := ds.NewKey(key)
		err = batch.Put(dsKey, []byte(key))
		assert.NoError(t, err)
	}

	cfg.BatchConcurrency = 100
	deleteKeyIdxs := []int{0, 8, 9, 22}
	for _, i := range deleteKeyIdxs {
		key := KeyPrefix + fmt.Sprint(i+1)
		dsKey := ds.NewKey(key)
		err = batch.Delete(dsKey)
		assert.NoError(t, err)
	}
	kBatch := batch.(*kodoBatch)
	numJobs := kBatch.CalcJobs()
	assert.Equal(t, keyCount+2, numJobs)

	err = batch.Commit()
	newKeyCount := keyCount*2 - 3
	assert.NoError(t, err)
	assert.Equal(t, newKeyCount, len(kodoApi.files))

	errorDelKey := KeyPrefix + fmt.Sprint(11)
	kodoApi.errorDel[obs.fixKey_(errorDelKey)] = struct{}{}

	keyStart += keyCount
	keyAdd := KeyPrefix + fmt.Sprint(keyStart+1)
	keyAddDs := ds.NewKey(keyAdd)
	keyDel := KeyPrefix + fmt.Sprint(keyStart-1)
	keyDelDs := ds.NewKey(keyDel)
	errorDelKeyDs := ds.NewKey(errorDelKey)

	fmt.Printf("last keyAdd:%v keyDel:%v errorDelKey:%v \n", keyAdd, keyDel, errorDelKey)

	batch, err = obs.Batch()
	assert.NoError(t, err)

	err = batch.Put(keyAddDs, []byte(keyAdd))
	assert.NoError(t, err)

	batch.Delete(keyDelDs)
	batch.Delete(errorDelKeyDs)

	err = batch.Commit()
	fmt.Printf("last batch commit err:%v \n", err)
	assert.Error(t, err)
	assert.Equal(t, newKeyCount, len(kodoApi.files))

}
