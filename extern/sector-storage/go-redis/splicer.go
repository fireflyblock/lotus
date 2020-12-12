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
	PubName          RedisKey = "pub"
	ParamsName       RedisKey = "params"
	PubTime          RedisKey = "pub_time"
	PubResName       RedisKey = "pub_res"
	ParamsResName    RedisKey = "params_res"
	RecoverName      RedisKey = "recover"
	RETRY            RedisKey = "retry"
	RecoveryWaitTime RedisKey = "recovery_wait_time"
	CountCacheFile   RedisKey = "count_cache_file_"
	CurrentTasks     RedisKey = "current_tasks_"
	WorkerCount      RedisKey = "worker_count*"
	WCT              RedisKey = "worker_count_"
	WorkerConfig     RedisKey = "worker_config*"
	WCF              RedisKey = "worker_config_"
	TFP              RedisKey = "transfer_failure_path_"

	SEALAPID = "seal_ap_"
	//FIELDPLEDGE    RedisField = "seal/v0/addpiece/pledge"
	//FIELDSEAL       RedisField = "seal/v0/addpiece/seal"
	//FIELDP1         RedisField = "seal/v0/precommit/1"
	//FIELDP2         RedisField = "seal/v0/precommit/2"
	//FIELDC1         RedisField = "seal/v0/commit/1"
	FIELDAP      RedisField = "ap"
	FIELDPLEDGE  RedisField = "pledge"
	FIELDSEAL    RedisField = "seal"
	FIELDP1      RedisField = "p1"
	FIELDP2      RedisField = "p2"
	FIELDC1      RedisField = "c1"
	RegisterTime RedisField = "register"

	PUBLISHCHANNELAP = "pub_cha_ap" //miner pub task
	PUBLISHCHANNELP1 = "pub_cha_p1" //miner pub task
	PUBLISHCHANNELP2 = "pub_cha_p2" //miner pub task
	PUBLISHCHANNELC1 = "pub_cha_c1" //miner pub task

	PubResSucceed = "s"
	PubResFailed  = "f"

	TASKRECORD = "task_*"
)

type RedisField string

//拼接发布任务和参数的field
func SplicingPubMessage(sectorID abi.SectorNumber, taskType sealtasks.TaskType, hostName string, sealApId uint64) RedisField {
	switch taskType {
	case sealtasks.TTAddPiecePl:
		str := fmt.Sprintf("%d_%s_%s", sectorID, FIELDPLEDGE, hostName)
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
		return FIELDAP
	case sealtasks.TTAddPiecePl:
		return FIELDPLEDGE
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
	case FIELDAP:
		return sealtasks.TTAddPiece
	case FIELDPLEDGE:
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

func (rf RedisField) ToTnt() int {
	res, err := strconv.Atoi(string(rf))
	if err != nil {
		log.Errorf("===== ToTnt err %+v", err)
	}
	return res
}

func (rf RedisField) ToSectorNumber() abi.SectorNumber {
	res, err := strconv.Atoi(string(rf))
	if err != nil {
		log.Errorf("===== ToTnt err %+v", err)
	}
	return abi.SectorNumber(res)
}

type RedisKey string

//拼接worker配置的key
func SplicingTaskCounntKey(hostName string) RedisKey {
	str := fmt.Sprintf("%s%s", WCT, hostName)
	return RedisKey(str)
}

//拼接counter_p1配置的key
func SplicingCounterP1Key(hostName string) RedisKey {
	str := fmt.Sprintf("%s%s", CountCacheFile, hostName)
	return RedisKey(str)
}

//拼接counter_p1配置的key
func SplicingCurrentTasks(hostName string) RedisKey {
	str := fmt.Sprintf("%s%s", CurrentTasks, hostName)
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
	str := fmt.Sprintf("%s%s", WCF, hostName)
	return RedisKey(str)
}

func SplicingChannel(hostName string) string {
	str := fmt.Sprintf("cha_%s", hostName)
	return str
}

func TypeToType(keys, type2 RedisField) RedisField {
	list1 := strings.Split(string(keys), "_")

	switch type2 {
	case FIELDPLEDGE:
		res := fmt.Sprintf("%s_%s", list1[0], "pledge")
		return RedisField(res)
	case FIELDSEAL:
		res := fmt.Sprintf("%s_%s", list1[0], "seal")
		return RedisField(res)
	case FIELDP1:
		res := fmt.Sprintf("%s_%s", list1[0], "p1")
		return RedisField(res)
	case FIELDP2:
		res := fmt.Sprintf("%s_%s", list1[0], "p2")
		return RedisField(res)
	case FIELDC1:
		res := fmt.Sprintf("%s_%s", list1[0], "c1")
		return RedisField(res)
	default:
		return ""
	}
}

func SplicingTransferFailurePath(hostname string) RedisKey {
	str := fmt.Sprintf("%s%s", TFP, hostname)
	return RedisKey(str)
}
