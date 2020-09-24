package goredis

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"strconv"
	"strings"
)

const (
	PUB_NAME        RedisKey = "pub"
	PARAMS_NAME     RedisKey = "params"
	PUB_RES_NAME    RedisKey = "pub_res"
	PARAMS_RES_NAME RedisKey = "params_res"
	WORKER_COUNT    RedisKey = "worker_count*"
	WORKER_CONFIG   RedisKey = "worker_config*"
	//FIELDPLEDGEP    RedisField = "seal/v0/addpiece/pledge"
	//FIELDSEAL       RedisField = "seal/v0/addpiece/seal"
	//FIELDP1         RedisField = "seal/v0/precommit/1"
	//FIELDP2         RedisField = "seal/v0/precommit/2"
	//FIELDC1         RedisField = "seal/v0/commit/1"
	FIELDPAP     RedisField = "ap"
	FIELDPLEDGEP RedisField = "pledge"
	FIELDSEAL    RedisField = "seal"
	FIELDP1      RedisField = "p1"
	FIELDP2      RedisField = "p2"
	FIELDC1      RedisField = "c1"

	PUBLISHCHANNELAP = "pub_cha_ap" //miner pub task
	PUBLISHCHANNELP1 = "pub_cha_p1" //miner pub task
	PUBLISHCHANNELP2 = "pub_cha_p2" //miner pub task
	PUBLISHCHANNELC1 = "pub_cha_c1" //miner pub task

	SUBSCRIBECHANNEL = "sub_cha" //worker pub res

)

type RedisField string

//拼接发布任务和参数的field
func SplicingPubMessage(sectorID abi.SectorNumber, taskType sealtasks.TaskType, hostName string, sealApId uint64) RedisField {
	switch taskType {
	case sealtasks.TTAddPiecePl:
		str := fmt.Sprintf("%d_%s_%s", sectorID, FIELDPLEDGEP, hostName)
		return RedisField(str)
	case sealtasks.TTAddPieceSe:
		str := fmt.Sprintf("%d_%s_%s_%d", sectorID, FIELDSEAL, hostName, sealApId)
		return RedisField(str)
	case sealtasks.TTPreCommit1:
		str := fmt.Sprintf("%d_%s_%s", sectorID, FIELDP1, hostName)
		return RedisField(str)
	case sealtasks.TTPreCommit2:
		str := fmt.Sprintf("%d_%s_%s", sectorID, FIELDP2, hostName)
		return RedisField(str)
	case sealtasks.TTCommit1:
		str := fmt.Sprintf("%d_%s_%s", sectorID, FIELDC1, hostName)
		return RedisField(str)
	default:
		return ""
	}
}

func SplicingBackupPubAndParamsField(sectorID abi.SectorNumber, taskType sealtasks.TaskType, sealApId uint64) RedisField {
	switch taskType {
	case sealtasks.TTAddPiecePl:
		str := fmt.Sprintf("%d_%s", sectorID, "pledge")
		return RedisField(str)
	case sealtasks.TTAddPieceSe:
		str := fmt.Sprintf("%d_%s_%d", sectorID, "deal", sealApId)
		return RedisField(str)
	case sealtasks.TTPreCommit1:
		str := fmt.Sprintf("%d_%s", sectorID, "p1")
		return RedisField(str)
	case sealtasks.TTPreCommit2:
		str := fmt.Sprintf("%d_%s", sectorID, "p2")
		return RedisField(str)
	case sealtasks.TTCommit1:
		str := fmt.Sprintf("%d_%s", sectorID, "c1")
		return RedisField(str)
	default:
		return RedisField("")
	}
}

func ToFieldTaskType(tt sealtasks.TaskType) RedisField {
	switch tt {
	case sealtasks.TTAddPiece:
		return FIELDPAP
	case sealtasks.TTAddPiecePl:
		return FIELDPLEDGEP
	case sealtasks.TTAddPieceSe:
		return FIELDSEAL
	case sealtasks.TTPreCommit1:
		return FIELDP1
	case sealtasks.TTPreCommit2:
		return FIELDP2
	case sealtasks.TTCommit1:
		return FIELDC1
	default:
		return ""
	}
}

//提取ield中的sid
func (rf RedisField) TailoredPubAndParamsfield() (sid abi.SectorNumber, taskType string, sealId uint64, err error) {
	res := strings.Split(string(rf), "_")

	switch len(res) {
	case 0:
		return 0, "", 0, errors.New("key Tailored err")
	case 2:
		resSid, _ := strconv.Atoi(res[0])
		sid = abi.SectorNumber(resSid)
		taskType = res[1]
		return
	case 3:
		resSid, _ := strconv.Atoi(res[0])
		sid = abi.SectorNumber(resSid)
		taskType = res[1]
		resId, _ := strconv.Atoi(res[2])
		sealId = uint64(resId)
		return
	default:
		return 0, "", 0, errors.New("key unrecognized")
	}

}

func (rf RedisField) TailoredSubMessage() (sid abi.SectorNumber, taskType RedisField, hostName string, sealId uint64, err error) {
	res := strings.Split(string(rf), "_")

	switch len(res) {
	case 0:
		return 0, "", "", 0, errors.New("key Tailored err")
	case 3:
		resSid, _ := strconv.Atoi(res[0])
		sid = abi.SectorNumber(resSid)
		resTaskType := res[1]
		taskType = RedisField(resTaskType)
		hostName = res[2]
		return
	case 4:
		resSid, _ := strconv.Atoi(res[0])
		sid = abi.SectorNumber(resSid)
		resTaskType := res[1]
		taskType = RedisField(resTaskType)
		hostName = res[2]
		resId, _ := strconv.Atoi(res[3])
		sealId = uint64(resId)
		return
	default:
		return 0, "", "", 0, errors.New("key unrecognized")
	}
}

func (rf RedisField) ToOfficalTaskType() sealtasks.TaskType {
	switch rf {
	case FIELDPAP:
		return sealtasks.TTAddPiece
	case FIELDPLEDGEP:
		return sealtasks.TTAddPiecePl
	case FIELDSEAL:
		return sealtasks.TTAddPieceSe
	case FIELDP1:
		return sealtasks.TTPreCommit1
	case FIELDP2:
		return sealtasks.TTPreCommit2
	case FIELDC1:
		return sealtasks.TTCommit1
	default:
		return ""
	}
}

func (rf RedisField) ToString() string {
	return string(rf)
}

type RedisKey string

//拼接worker配置的key
func SplicingTaskCounntKey(hostName string) RedisKey {
	str := fmt.Sprintf("worker_count_%s", hostName)
	return RedisKey(str)
}

//提取hostname
func (rk RedisKey) TailoredWorker() (hostname string) {
	res := strings.Split(string(rk), "_")
	if len(res) == 3 {
		return res[2]
	}
	return ""
}

func (rk RedisKey) ToString() string {
	return string(rk)
}

//拼接worker计数的key
func SplicingTaskConfigKey(hostName string) RedisKey {
	str := fmt.Sprintf("worker_config_%s", hostName)
	return RedisKey(str)
}

func SplicingChannel(hostName string) string {
	str := fmt.Sprintf("cha_%s", hostName)
	return str
}
