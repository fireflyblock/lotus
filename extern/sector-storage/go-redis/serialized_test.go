package goredis

import (
	"github.com/filecoin-project/go-state-types/abi"
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
