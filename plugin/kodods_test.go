package plugin

import (
	"reflect"
	"testing"

	kodods "github.com/ldcsoftware/ipfs-ds-kodo"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func TestKodoPluginDatastoreConfigParser(t *testing.T) {
	testcases := []struct {
		Input  map[string]interface{}
		Want   *KodoConfig
		HasErr bool
	}{
		{
			// Default case
			Input: map[string]interface{}{
				"ucHosts": []string{"123", "456"},
				"bucket":  "somebucket",
				"ak":      "someaccesskey",
				"sk":      "somesecretkey",
			},
			Want: &KodoConfig{cfg: &kodods.Config{
				Config: &operation.Config{
					UcHosts: []string{"123", "456"},
					Bucket:  "somebucket",
					Ak:      "someaccesskey",
					Sk:      "somesecretkey",
				},
			}},
		},
		{
			// Required fields missing
			Input: map[string]interface{}{
				"bucket": "someregion",
			},
			HasErr: true,
		},
		{
			// Optional fields included
			Input: map[string]interface{}{
				"ucHosts":   []string{"123", "456"},
				"bucket":    "somebucket",
				"ak":        "someaccesskey",
				"sk":        "somesecretkey",
				"retry":     3,
				"listLimit": 4,
			},
			Want: &KodoConfig{cfg: &kodods.Config{
				Config: &operation.Config{
					UcHosts: []string{"123", "456"},
					Bucket:  "somebucket",
					Ak:      "someaccesskey",
					Sk:      "somesecretkey",
					Retry:   3,
				},
				ListLimit: 4,
			}},
		},
	}

	for i, tc := range testcases {
		cfg, err := KodoPlugin{}.DatastoreConfigParser()(tc.Input)
		if err != nil {
			if tc.HasErr {
				continue
			}
			t.Errorf("case %d: Failed to parse: %s", i, err)
			continue
		}
		if got, ok := cfg.(*KodoConfig); !ok {
			t.Errorf("wrong config type returned: %T", cfg)
		} else if !reflect.DeepEqual(got, tc.Want) {
			t.Errorf("case %d: got: %v; want %v", i, got, tc.Want)
		}
	}
}
