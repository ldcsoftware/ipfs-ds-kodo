package plugin

import (
	"fmt"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	kodods "github.com/ldcsoftware/ipfs-ds-kodo"
	"github.com/ldcsoftware/qiniu-go-sdk/syncdata/operation"
)

var Plugins = []plugin.Plugin{
	&KodoPlugin{},
}

type KodoPlugin struct{}

func (p KodoPlugin) Name() string {
	return "kodo-datastore-plugin"
}

func (p KodoPlugin) Version() string {
	return "0.0.1"
}

func (p KodoPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (p KodoPlugin) DatastoreTypeName() string {
	return "kodods"
}

func (p KodoPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		fmt.Printf("datastore config parser m:%+v \n", m)
		fmt.Printf("datastore config parser m:%T \n", m["ucHosts"])

		ucHostsOri, ok := m["ucHosts"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("kodo: no ucHosts specified")
		}
		ucHosts := make([]string, 0, len(ucHostsOri))
		for _, ucHostUri := range ucHostsOri {
			ucHost, ok := ucHostUri.(string)
			if !ok {
				return nil, fmt.Errorf("kodo: no ucHost specified")
			}
			ucHosts = append(ucHosts, ucHost)
		}

		bucket, ok := m["bucket"].(string)
		if !ok {
			return nil, fmt.Errorf("kodo: no bucket specified")
		}

		accessKey, ok := m["ak"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no accessKey specified")
		}

		secretKey, ok := m["sk"].(string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no secretKey specified")
		}

		retry, _ := m["retry"].(int)
		punishTimeS, _ := m["punishTimeS"].(int)
		dialTimeoutMs, _ := m["dialTimeoutMs"].(int)
		prefix, _ := m["prefix"].(string)
		listLimit, _ := m["listLimit"].(int)
		deleteLimit, _ := m["deleteLimit"].(int)
		batchConcurrency, _ := m["batchConcurrency"].(int)

		return &KodoConfig{
			cfg: &kodods.Config{
				Config: &operation.Config{
					UcHosts:       ucHosts,
					Bucket:        bucket,
					Ak:            accessKey,
					Sk:            secretKey,
					Retry:         retry,
					PunishTimeS:   punishTimeS,
					DialTimeoutMs: dialTimeoutMs,
				},
				Prefix:           prefix,
				ListLimit:        listLimit,
				DeleteLimit:      deleteLimit,
				BatchConcurrency: batchConcurrency,
			},
		}, nil
	}
}

type KodoConfig struct {
	cfg *kodods.Config
}

func (c *KodoConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"bucket": c.cfg.Bucket,
		"prefix": c.cfg.Prefix,
	}
}

func (c *KodoConfig) Create(path string) (repo.Datastore, error) {
	return kodods.NewKodoDatastore(c.cfg)
}
