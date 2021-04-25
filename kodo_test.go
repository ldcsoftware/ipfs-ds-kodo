package kodods

import (
	"testing"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

type mockKodoApi struct {
}

func TestKodoDs(t *testing.T) {

}

func TestBatchOp(t *testing.T) {
	var partSize int64 = 4
	var chunkSize int = 1
	keyPrefix := "testkey"

	cfg := &Config{
		Config: &operation.Config{},
	}
	cfg.fixConfig()

	kodoApi := &mockKodoApi{
		uploadCtx:   make(map[string]*uploadCtx),
		concurrency: 32,
		partSize:    cfg.PartSize,
	}

	obs := &kodoObs{
		Config:     cfg,
		uploader:   kodoApi,
		downloader: kodoApi,

		suggestedPartSize: cfg.PartSize,
	}
	obs.cbPool = NewChunkBufferPool(cfg.ChunkSize, 4)

}
