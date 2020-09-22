package goredis

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/ipfs/go-cid"
	"testing"
)

func TestSerialized(t *testing.T) {

	//ctx ,_:= context.WithCancel(context.Background())
	sid := abi.SectorID{10020, 99}
	sr := abi.SealRandomness{1, 2, 3, 4, 5}
	pieces := []abi.PieceInfo{
		abi.PieceInfo{512, cid.Cid{}},
		abi.PieceInfo{1024, cid.Cid{}},
	}
	pp1 := ParamsP1{
		//Ctx:     ctx,
		Sector:  sid,
		Ticket:  sr,
		Pieces:  pieces,
		Recover: true,
	}
	t.Log("pp1:", pp1)
	res1, err := Serialization(pp1)
	if err != nil {
		t.Errorf("ser err %+v", err)
	}
	t.Log("res1:", res1)

	var pp2 = &ParamsP1{}
	err = Deserialization(res1, pp2)
	if err != nil {
		t.Errorf("des err %+v", err)
	}
	t.Log("pp2:", pp2)

}

var (
	RedisURL = []string{
		"172.16.0.7:8001",
		"172.16.0.7:8002",
		"172.16.0.8:8001",
	}
	RedisPassWord = "rcQuwPzASm"
)

func TestHset(t *testing.T) {
	ctx := context.Background()
	rc := NewRedisClusterCLi(ctx, RedisURL, RedisPassWord)
	if rc == nil {
		t.Errorf("new redis cluster client err")
	}

	field := SplicingBackupPubAndParamsField(48, sealtasks.TTPreCommit1, 0)
	err := rc.HSet(PUB_RES_NAME, field, "ff-203-worker8")
	if err != nil {
		t.Errorf("hset err")
	}
}

func TestPublish(t *testing.T) {
	channel := "sub_cha"
	hostName := "ff-203-worker8"

	ctx := context.Background()
	rc := NewRedisClusterCLi(ctx, RedisURL, RedisPassWord)
	if rc == nil {
		t.Errorf("new redis cluster client err")
	}
	pubMsg := SplicingPubMessage(48, sealtasks.TTPreCommit1, hostName, 0)
	res, err := rc.Publish(channel, pubMsg)
	if err != nil {
		t.Errorf("pub err")
	}
	t.Log("res => ", res)

}
