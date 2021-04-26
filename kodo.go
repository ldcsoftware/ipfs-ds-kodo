package kodods

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"

	"github.com/ldcsoftware/qiniu-go-sdk/api.v8/kodo"
	"github.com/ldcsoftware/qiniu-go-sdk/syncdata/operation"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type iUploader interface {
	UploadData(ctx context.Context, key string, data []byte, ret interface{}) (err error)
}

type iDownloader interface {
	DownloadBytes(key string) (data []byte, err error)
}

type iLister interface {
	Stat(ctx context.Context, key string) (entry kodo.Entry, err error)
	Delete(ctx context.Context, key string) (err error)
	BatchDelete(ctx context.Context, keys ...string) (rets []kodo.BatchItemRet, err error)
	ListPrefix(ctx context.Context, prefix, marker string, limit int) (entrys []kodo.ListItem, markerOut string, err error)
}

type httpCoder interface {
	HttpCode() int
}

type Config struct {
	*operation.Config
	Prefix           string `json:"prefix"`
	ListLimit        int    `json:"list_limit"`
	DeleteLimit      int    `json:"delete_limit"`
	BatchConcurrency int    `json:"batch_concurrency"`
}

func (cfg *Config) fixConfig() {
	if cfg.Config == nil {
		log.Fatal("kodo config is invalid")
	}

	if len(cfg.UcHosts) == 0 {
		log.Fatal("kodo uc config is invalid")
	}

	if cfg.UpConcurrency == 0 {
		cfg.UpConcurrency = 1
	}
	if cfg.PartSize == 0 {
		cfg.PartSize = 4
	}
	if cfg.Retry == 0 {
		cfg.Retry = 30
	}
	if cfg.PunishTimeS == 0 {
		cfg.PunishTimeS = 10
	}
	if cfg.DialTimeoutMs == 0 {
		cfg.DialTimeoutMs = 50
	}
	if cfg.ListLimit == 0 {
		cfg.ListLimit = 1000
	}
	if cfg.DeleteLimit == 0 {
		cfg.DeleteLimit = 1000
	}
	if cfg.BatchConcurrency == 0 {
		cfg.BatchConcurrency = 100
	}
	cfg.PartSize = cfg.PartSize * 1024 * 1024
}

type KodoDs struct {
	*Config
	uploader   iUploader
	downloader iDownloader
	lister     iLister
}

func NewKodoDatastore(cfg *Config) (*KodoDs, error) {
	cfg.fixConfig()

	uploader := operation.NewUploader(cfg.Config)
	downloader := operation.NewDownloader(cfg.Config)
	lister := operation.NewLister(cfg.Config)

	s := &KodoDs{
		Config:     cfg,
		uploader:   uploader,
		downloader: downloader,
		lister:     lister,
	}

	log.Printf("New kodo datastore cfg:%+v sdk:%+v \n", cfg, cfg.Config)
	return s, nil
}

func (s *KodoDs) Put(key ds.Key, value []byte) error {
	log.Printf("kodo ds put key:%v \n", key)
	return s.uploader.UploadData(context.Background(), s.fixKey(key), value, nil)
}

func (s *KodoDs) Sync(key ds.Key) error {
	log.Printf("kodo ds sync key:%v \n", key)
	return nil
}

func (s *KodoDs) Get(key ds.Key) ([]byte, error) {
	log.Printf("kodo ds get key:%v \n", key)
	data, err := s.downloader.DownloadBytes(s.fixKey(key))
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return data, err
}

func (s *KodoDs) Has(key ds.Key) (exists bool, err error) {
	log.Printf("kodo ds has key:%v \n", key)
	_, err = s.GetSize(key)
	if err != nil {
		if err == ds.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *KodoDs) GetSize(key ds.Key) (size int, err error) {
	log.Printf("kodo ds get size key:%v \n", key)
	entry, err := s.lister.Stat(context.Background(), s.fixKey(key))
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return int(entry.Fsize), nil
}

func (s *KodoDs) Delete(key ds.Key) error {
	log.Printf("kodo ds delete key:%v \n", key)
	err := s.lister.Delete(context.Background(), s.fixKey(key))
	if isNotFound(err) {
		err = nil
	}
	return err
}

func (s *KodoDs) Query(q dsq.Query) (dsq.Results, error) {
	log.Printf("kodo ds query key:%v \n", q.Prefix)

	var entrys []kodo.ListItem
	var marker string
	var err error

	qNaive := q
	qNaive.Prefix = ""
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		q.Prefix = s.fixKey_(prefix) + "/"
	}

	nextValue := func() (dsq.Result, bool) {
		if len(entrys) == 0 {
			entrys, marker, err = s.lister.ListPrefix(context.Background(), s.fixKey_(q.Prefix), marker, s.ListLimit)
			if err != nil {
				return dsq.Result{Error: err}, false
			}
		}
		entry := dsq.Entry{
			Key:  ds.NewKey(entrys[0].Key).String(),
			Size: int(entrys[0].Fsize),
		}
		entrys = entrys[1:]
		if !q.KeysOnly {
			value, err := s.Get(ds.NewKey(entry.Key))
			if err != nil {
				return dsq.Result{Error: err}, false
			}
			entry.Value = value
		}
		return dsq.Result{Entry: entry}, true
	}

	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

func (s *KodoDs) Close() error {
	return nil
}

func (s *KodoDs) fixKey(key ds.Key) string {
	return path.Join(s.Prefix, key.String())
}

func (s *KodoDs) fixKey_(key string) string {
	return path.Join(s.Prefix, key)
}

func (s *KodoDs) Batch() (ds.Batch, error) {
	log.Printf("kodo ds batch \n")
	return &kodoBatch{
		s:           s,
		puts:        make(map[ds.Key][]byte),
		deletes:     make([]ds.Key, 0),
		deleteLimit: s.DeleteLimit,
		concurrency: s.BatchConcurrency,
	}, nil
}

func isNotFound(err error) bool {
	var hc httpCoder
	return errors.As(err, &hc) && isNotFoundCode(hc.HttpCode())
}

func isNotFoundCode(code int) bool {
	return code == 404 || code == 612
}

type kodoBatch struct {
	s           *KodoDs
	puts        map[ds.Key][]byte
	deletes     []ds.Key
	deleteLimit int
	concurrency int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (b *kodoBatch) Put(key ds.Key, val []byte) error {
	log.Printf("kodo ds batch put key:%v \n", key)
	b.puts[key] = val
	return nil
}

func (b *kodoBatch) Delete(key ds.Key) error {
	log.Printf("kodo ds batch delete key:%v \n", key)
	b.deletes = append(b.deletes, key)
	return nil
}

func (b *kodoBatch) CalcJobs() int {
	return len(b.puts) + ((len(b.deletes) + b.deleteLimit - 1) / b.deleteLimit)
}

func (b *kodoBatch) Commit() error {
	log.Printf("kodo ds batch commit \n")
	var (
		deleteKeys []string = make([]string, 0, len(b.deletes))
	)
	for _, key := range b.deletes {
		deleteKeys = append(deleteKeys, b.s.fixKey(key))
	}

	numJobs := b.CalcJobs()
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	concurrency := b.concurrency
	if numJobs < concurrency {
		concurrency = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	defer wg.Wait()

	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for k, val := range b.puts {
		jobs <- b.newPutJob(k, val)
	}

	for i := 0; i < len(deleteKeys); i += b.deleteLimit {
		limit := b.deleteLimit
		if len(deleteKeys[i:]) < limit {
			limit = len(deleteKeys[i:])
		}
		jobs <- b.newDeleteJob(deleteKeys[i : i+limit])
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		err := <-results
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("kodods: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}

func (b *kodoBatch) newPutJob(k ds.Key, value []byte) func() error {
	return func() error {
		return b.s.Put(k, value)
	}
}

func (b *kodoBatch) newDeleteJob(deletekeys []string) func() error {
	return func() error {
		rets, err := b.s.lister.BatchDelete(context.Background(), deletekeys...)
		if err != nil && !isNotFound(err) {
			return err
		}

		var errs []string
		for _, ret := range rets {
			if ret.Code != 200 && !isNotFoundCode(ret.Code) {
				errs = append(errs, ret.Error)
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("failed to delete objects: %s", errs)
		}
		return nil
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

var _ ds.Batching = (*KodoDs)(nil)
