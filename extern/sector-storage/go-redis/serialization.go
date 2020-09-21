package goredis

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"reflect"
	"strconv"
)

type ParamsAp struct {
	//Ctx          context.Context         `json:"ctx"`
	Sector       abi.SectorID            `json:"sector"`
	PieceSizes   []abi.UnpaddedPieceSize `json:"piece_sizes"`
	NewPieceSize abi.UnpaddedPieceSize   `json:"new_piece_size"`
	PieceData    storage.Data            `json:"piece_data"`
	//FileName     string                  `json:"file_name"`
	//FilePath     string                  `json:"file_path"`
	ApType string `json:"ap_type"`
}

type ParamsResAp struct {
	PieceInfo abi.PieceInfo `json:"piece_info"`
	Err       error         `json:"err"`
}

type ParamsP1 struct {
	//Ctx     context.Context    `json:"ctx"`
	Sector  abi.SectorID       `json:"sector"`
	Ticket  abi.SealRandomness `json:"ticket"`
	Pieces  []abi.PieceInfo    `json:"pieces"`
	Recover bool               `json:"recover"`
}

type ParamsResP1 struct {
	Out storage.PreCommit1Out `json:"out"`
	Err error                 `json:"err"`
}

type ParamsP2 struct {
	//Ctx    context.Context
	Sector abi.SectorID          `json:"sector"`
	Pc1o   storage.PreCommit1Out `json:"pc_1_o"`
}

type ParamsResP2 struct {
	Out storage.SectorCids `json:"out"`
	Err error              `json:"err"`
}

type ParamsC1 struct {
	//Ctx    context.Context
	Sector   abi.SectorID                  `json:"sector"`
	Ticket   abi.SealRandomness            `json:"ticket"`
	Seed     abi.InteractiveSealRandomness `json:"seed"`
	Pieces   []abi.PieceInfo               `json:"pieces"`
	Cids     storage.SectorCids            `json:"cids"`
	PathList []string                      `json:"path_list"`
}

type ParamsResC1 struct {
	Out         storage.Commit1Out `json:"out"`
	StoragePath string             `json:"storage_path"`
	Err         error              `json:"err"`
}

type ParamsC2 struct {
	//Ctx    context.Context
	Sector abi.SectorID       `json:"sector"`
	C1o    storage.Commit1Out `json:"c_1_o"`
}

func Serialization(value interface{}) ([]byte, error) {
	if bytes, ok := value.([]byte); ok {
		return bytes, nil
	}

	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []byte(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Map, reflect.Array, reflect.Struct:
	}
	k, err := json.Marshal(value)
	return k, err
}

func Deserialization(byt []byte, ptr interface{}) (err error) {
	if bytes, ok := ptr.(*[]byte); ok {
		*bytes = byt
		return
	}
	if v := reflect.ValueOf(ptr); v.Kind() == reflect.Ptr {
		switch p := v.Elem(); p.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			var i int64
			i, err = strconv.ParseInt(string(byt), 10, 64)
			if err != nil {
				fmt.Printf("Deserialization: failed to parse int '%s': %s", string(byt), err)
			} else {
				p.SetInt(i)
			}
			return

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			var i uint64
			i, err = strconv.ParseUint(string(byt), 10, 64)
			if err != nil {
				fmt.Printf("Deserialization: failed to parse uint '%s': %s", string(byt), err)
			} else {
				p.SetUint(i)
			}
			return
		}
	}
	err = json.Unmarshal(byt, &ptr)
	return
}
