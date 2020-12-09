mkdir bin scripts config

git clone -b develop-post git@192.168.20.201:filecoin-project/lotus.git
git clone -b proofs-v5.4.0 git@192.168.20.201:firefly-rust/fil-proofs.git
git clone -b cmd git@192.168.20.201:filecoin-project/redis-tools.git
git clone -b dev git@192.168.20.201:filecoin-project/schedu.git
git clone -b dev git@192.168.20.201:filecoin-project/go-fil-markets.git
git clone -b dev git@192.168.20.201:filecoin-project/specs-storage.git

export FFI_BUILD_FROM_SOURCE=1
export IPFS_GATEWAY="https://proof-parameters.s3.cn-south-1.jdcloud-oss.com/ipfs/"

cd lotus || exit
make
cp lotus lotus-wallet && cp lotus lotus-shed
mv lotus lotus-miner lotus-shed lotus-wallet ../bin/
make clean
chmod +x ./circleci.sh ||exit
./circleci.sh | tee /tmp/dev-post.log

cd ..
cd fil-proofs/filecoin-ffi/rust || exit
cargo build --release
cp ./target/release/libfilcrypto.a ../../../lotus/extern/filecoin-ffi/

cd ../../../lotus || exit
git checkout -f develop
mv extern/sector-storage/grpc/config/default.toml ../config/
make lotus-miner lotus-worker
cp lotus-miner lotus-miner-sched && cp lotus-worker lotus-worker-appc
mv lotus-miner-sched lotus-worker-appc ../bin/
make clean
chmod +x ./circleci.sh ||exit
./circleci.sh | tee /tmp/dev-schedu.log

cd ..
cd fil-proofs/filecoin-ffi/rust || exit
RUSTFLAGS="-C target-cpu=corei7" cargo build --release
cp ./target/release/libfilcrypto.a ../../../lotus/extern/filecoin-ffi/

cd ../../../lotus || exit
make lotus-worker
cp lotus-worker lotus-worker-c2
mv lotus-worker-c2 ../bin/
make clean

cd ..
cd redis-tools || exit
go build && mv redistool ../bin/

cd ..
cd schedu || exit
go build && mv schedu ../bin/
mv ./schedu.toml ../config/

cd ..
tar czvf firefly.tar.gz bin config scripts
