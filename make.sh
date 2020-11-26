#! /bin/bash

# 编译lotus
cd ./official/lotus

make lotus

cp ./lotus ../../../products/lotus

#编译miner

cd ../../firefly/fil-proofs/filecoin-ffi/rust

export FFI_BUILD_FROM_SOURCE=1

cargo build --release 

cp ./target/release/libfilcrypto.a ../../../lotus/extern/filecoin-ffi/

cd ../../../lotus

make clean

make lotus-miner lotus-worker

cp ./lotus-miner ../../../products/lotus-miner

#编译worker-sdr

#cd ../fil-proofs-sdr/filecoin-ffi/rust

#export FFI_BUILD_FROM_SOURCE=1

#cargo build --release

#cp ./target/release/libfilcrypto.a ../../../lotus/extern/filecoin-ffi/

#cd ../../../lotus

#make lotus-worker

cp ./lotus-worker  ../../../products/lotus-worker-sdr

#编译worker-c2

cd ../fil-proofs/filecoin-ffi/rust

RUSTFLAGS="-C target-cpu=corei7" cargo build --release

cp ./target/release/libfilcrypto.a ../../../lotus/extern/filecoin-ffi/

cd ../../../lotus

make clean

make lotus-worker

cp ./lotus-worker  ../../../products/lotus-worker-c2

#压缩
cd ../../../products
tar -zcvf lotus.tar.gz lotus lotus-miner lotus-worker-c2 lotus-worker-sdr
