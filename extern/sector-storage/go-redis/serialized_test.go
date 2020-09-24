package goredis

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	logrus "github.com/filecoin-project/sector-storage/log"
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

var (
	DefaultRedisURL = []string{
		"172.16.0.7:8001",
		"172.16.0.7:8002",
		"172.16.0.8:8001",
	}
	DefaultRedisPassWord = "rcQuwPzASm"
)

func TestDelete(t *testing.T) {
	ctx := context.Background()
	cc := NewRedisClusterCLi(ctx, DefaultRedisURL, DefaultRedisPassWord)
	var sectorID abi.SectorNumber = 1
	sealKey := RedisKey(fmt.Sprintf("seal_ap_%d", sectorID))
	tasklist := make([]sealtasks.TaskType, 0)

	c, err := cc.Exist(sealKey)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}
	if c > 0 {
		list := []sealtasks.TaskType{sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		tasklist = append(tasklist, list...)
	} else {

		list := []sealtasks.TaskType{sealtasks.TTAddPiecePl, sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		tasklist = append(tasklist, list...)
	}
	fmt.Println(tasklist)

	//1 backup params
	for _, v := range tasklist {
		f := SplicingBackupPubAndParamsField(sectorID, v, 0)
		//2 backup  pub
		count, err := cc.HDel(PARAMS_NAME, f)
		t.Logf("delete params field:%+v count:%d", f, count)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd DeleteDataForSid err, sectorID %+v err %+v", sectorID, err)
		}
		//3 backup res params
		count, err = cc.HDel(PUB_NAME, f)
		t.Logf("delete pub field:%+v count:%d", f, count)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd DeleteDataForSid err, sectorID %+v err %+v", sectorID, err)
		}
		//4 backup res
		count, err = cc.HDel(PUB_RES_NAME, f)
		t.Logf("delete pub res field:%+v count:%d", f, count)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd DeleteDataForSid err, sectorID %+v err %+v", sectorID, err)
		}
		//5 seal_ap_%d
		count, err = cc.HDel(PARAMS_RES_NAME, f)
		t.Logf("delete params res field:%+v count:%d", f, count)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd DeleteDataForSid err, sectorID %+v err %+v", sectorID, err)
		}
	}

	if c == 0 {
		return
	}

	logrus.SchedLogger.Infof("===== rd DeleteDataForSid, sectorID %+v sealKey %+v", sectorID, sealKey)
	var count int64
	err = cc.Get(sealKey, &count)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd DeleteDataForSid, get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}

	cc.Del(sealKey)

	var i int64 = 1
	for i = 1; i <= count; i++ {
		f := SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, uint64(i))
		//2 backup  pub
		cc.HDel(PARAMS_NAME, f)
		//3 backup res params
		cc.HDel(PUB_NAME, f)
		//4 backup res
		cc.HDel(PUB_RES_NAME, f)
		//5 seal_ap_%d
		cc.HDel(PUB_RES_NAME, f)
	}

}
