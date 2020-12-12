package sectorstorage

import (
	"fmt"
	"testing"
)

func buildRedisWorker(path string) *RedisWorker {
	return &RedisWorker{
		sealer:     nil,
		redisCli:   nil,
		hostName:   "",
		workerPath: path,
	}
}

func TestRedisWorker_CheckLocalSectors(t *testing.T) {
	rw := buildRedisWorker("./worker-path")
	sids := rw.CheckLocalSectors()
	for _, sid := range sids {
		fmt.Printf("sid :%+v\n", sid)
	}
}

func TestRedisWorker_DeleteLocalSectors(t *testing.T) {
	rw := buildRedisWorker("./worker-path")
	sids := rw.CheckLocalSectors()
	for _, sid := range sids {
		fmt.Printf("sid :%+v\n", sid)
	}

	rw.DeleteLocalSectors(sids)
}
