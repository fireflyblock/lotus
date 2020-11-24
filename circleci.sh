#!/bin/bash

# go mod
function tidy() {
  go mod tidy -v
}

# check git diff
function git_diff() {
  git --no-pager diff go.mod go.sum
  git --no-pager diff --quiet go.mod go.sum
}

#lint
function go_lint() {
  golangci-lint run -v --timeout 2m --concurrency 16
}

#go fmt
#shellcheck disable=SC2162
function go_fmt() {
  ! go fmt ./... 2>&1 | read
}

#make debug
function make_debug() {
  make debug
}

#build
function build() {
  sudo apt install ocl-icd-opencl-dev libhwloc-dev
  git submodule sync
  git submodule update --init
}

# cd extern/oni && git submodule sync
function git_sync() {
  cd extern/oni && git submodule sync
  cd ../..
}

#cd extern/oni && git submodule update --init
function git_update() {
  cd extern/oni && git submodule update --init
  cd ../..
}

#cd extern/filecoin-ffi && make
function _make() {
  cd extern/filecoin-ffi && make
  cd ../..
}

#cd extern/oni/lotus-soup && go mod edit -replace github.com/filecoin-project/lotus=../../../ && go mod edit -replace github.com/filecoin-project/filecoin-ffi=../../filecoin-ffi && go mod edit -replace github.com/supranational/blst=../../blst
function make_all() {
  cd extern/oni/lotus-soup && go mod edit -replace github.com/filecoin-project/lotus=../../../ && go mod edit -replace github.com/filecoin-project/filecoin-ffi=../../filecoin-ffi && go mod edit -replace github.com/supranational/blst=../../blst
  cd ../../..
}

#pushd extern/oni/lotus-soup && go build -tags=testground .
function test_ground() {
  pushd extern/oni/lotus-soup && go build -tags=testground .
  cd ../../..
}

#make deps
function make_deps() {
  make deps
}

#make deps lotus
function make_deps_lotus() {
  make deps lotus
}

#go generate ./...
function go_generate() {
  go generate ./...
}

#git --no-pager diff --quiet
function git_quiet() {
  git --no-pager diff
  git --no-pager diff --quiet
}

#test-window-post
function test_window_post() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -run=TestWindowedPost \
    ./...
}

function test_storage() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -timeout 30m \
    ./storage/... ./extern/...
}

function test_short() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    --timeout 10m --short \
    ./...
}

function test_node() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -timeout 30m \
    ./node/...
}

function test_conformance_bleeding_edge() {
  gotestsum \
    -- \
    -v -coverpkg ./chain/vm/,github.com/filecoin-project/specs-actors/... -coverprofile=/tmp/conformance.out ./conformance/
}

function test_conformance() {
  gotestsum \
    -- \
    -v -coverpkg ./chain/vm/,github.com/filecoin-project/specs-actors/... -coverprofile=/tmp/conformance.out ./conformance/
}

function test_cli() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -timeout 30m \
    ./cli/... ./cmd/... ./api/...
}

function test_chain() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -timeout 30m \
    ./chain/...
}

function test() {
  gotestsum \
    --format pkgname-and-test-fails \
    -- \
    -coverprofile=coverage.txt -coverpkg=github.com/filecoin-project/lotus/... \
    -timeout 30m \
    ./...
}

function go_test() {
  go test ./...
}

echo "tidy start"
tidy
echo "tidy end"

echo "git_diff start"
git_diff
echo "git_diff end"

echo "go_lint start"
go_lint
echo "go_lint end"

echo "go_fmt start"
go_fmt
echo "go_fmt end"

echo "make_debug start"
make_debug
echo "make_debug end"

echo "build start"
build
echo "build end"

echo "git_sync start"
git_sync
echo "git_sync end"

echo "git_update start"
git_update
echo "git_update end"

echo "_make start"
_make
echo "_make end"

echo "make_all start"
make_all
echo "make_all end"

echo "test_ground start"
test_ground
echo "test_ground end"

echo "make_deps start"
make_deps
echo "make_deps end"

echo "make_deps_lotus start"
make_deps_lotus
echo "make_deps_lotus end"

echo "go_generate start"
go_generate
echo "go_generate end"

echo "git_quiet start"
git_quiet
echo "git_quiet end"

echo "test_window_post start"
test_window_post
echo "test_window_post end"

echo "test_storage start"
test_storage
echo "test_storage end"

echo "test_short start"
test_short
echo "test_short end"

echo "test_node start"
test_node
echo "test_node end"

echo "test_conformance_bleeding_edge start"
test_conformance_bleeding_edge
echo "test_conformance_bleeding_edge end"

echo "test_conformance start"
test_conformance
echo "test_conformance end"

echo "test_cli start"
test_cli
echo "test_cli end"

echo "test_chain start"
test_chain
echo "test_chain end"

echo "test start"
test
echo "test end"

#echo "go_test start"
#go_test
#echo "go_test end"
