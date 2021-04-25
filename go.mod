module github.com/ldcsoftware/ipfs-ds-kodo

go 1.15

replace github.com/qiniupd/qiniu-go-sdk => ./github.com/qiniupd/qiniu-go-sdk

require (
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ipfs v0.8.0
	github.com/kirsle/configdir v0.0.0-20170128060238-e45d2f54772f // indirect
	github.com/qiniupd/qiniu-go-sdk v1.0.2
	github.com/stretchr/testify v1.7.0
)
