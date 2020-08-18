module github.com/filecoin-project/sector-storage

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/detailyang/go-fallocate v0.0.0-20180908115635-432fa640bd2e
	github.com/elastic/go-sysinfo v1.3.0
	github.com/filecoin-project/filecoin-ffi v0.30.4-0.20200716204036-cddc56607e1d
	github.com/filecoin-project/go-bitfield v0.2.0
	github.com/filecoin-project/go-fil-commcid v0.0.0-20200716160307-8f644712406f
	github.com/filecoin-project/go-fil-markets v0.5.6
	github.com/filecoin-project/go-padreader v0.0.0-20200210211231-548257017ca6
	github.com/filecoin-project/go-paramfetch v0.0.2-0.20200218225740-47c639bab663
	github.com/filecoin-project/specs-actors v0.8.7-0.20200811203034-272d022c1923
	github.com/filecoin-project/specs-storage v0.1.1-0.20200622113353-88a9704877ea
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/hashicorp/go-multierror v1.1.0
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-log v1.0.4
	github.com/ipfs/go-log/v2 v2.0.5
	github.com/mitchellh/go-homedir v1.1.0
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.3
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.25.0
)

replace github.com/filecoin-project/storage-fsm => ../storage-fsm

replace github.com/filecoin-project/filecoin-ffi => ../filecoin-ffi

replace github.com/filecoin-project/specs-storage => ../../../specs-storage
