package sectorstorage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"

	"net/http"
	"runtime/debug"
	"time"
)

const defaultRequestURL = "http://192.168.20.158:8080/v0/firefly"

type TaskRecord struct {
	SectorId  uint64    `json:"sector_id"`
	TaskType  uint8     `json:"task_type"`
	Worker    string    `json:"worker"`
	MinerId   uint64    `json:"miner_id"`
	Status    uint8     `json:"status"`
	StoreTime time.Time `json:"store_time"`
}

type IncompleteTask struct {
	Sector uint64 `json:"sector_id"`
	Worker string `json:"worker"`
}

func (sh *scheduler) StartStore(sectorId abi.SectorNumber, taskType sealtasks.TaskType, worker string, minerId abi.ActorID, status TaskStatus, storeTime time.Time) {
	defer func() {
		err := recover()
		if err != nil {
			log.Error("StartStore panic:", err)
			s := debug.Stack()
			log.Error("StartStore Stack:", s)
		}
	}()
	var tt TaskType
	switch taskType {
	case sealtasks.TTAddPiece:
		tt = TT_ADDPIECE
	case sealtasks.TTPreCommit1:
		tt = TT_PRECOMMIT1
	case sealtasks.TTPreCommit2:
		tt = TT_PRECOMMIT2
	case sealtasks.TTCommit1:
		tt = TT_COMMIT1
	case sealtasks.TTCommit2:
		tt = TT_COMMIT2
	default:
		return
	}

	tr := TaskRecord{
		uint64(sectorId),
		uint8(tt),
		worker,
		uint64(minerId),
		uint8(status),
		storeTime,
	}

	data, err := json.Marshal(tr)
	if err != nil {
		log.Errorf("json Marshal err:", err)
		return
	}
	err = Store(data)
	if err != nil {
		log.Errorf("Store data err:", err)
		return
	}
}

func (sh *scheduler) StartLoad() error {
	itList, err := Load()
	if err != nil {
		return xerrors.Errorf("adding local worker: %w", err)
	}

	if len(itList) < 1 {
		return nil
	}

	log.Debugf("===== init taskRecorder :%+v", sh.taskRecorder)
	for _, it := range itList {
		tr := sectorTaskRecord{}
		tr.workerFortask = it.Worker
		sh.taskRecorder.Store(it.Sector, tr)
		log.Debugf("===== sectorTaskRecord :%+v", tr)
	}
	return nil
}

func Store(data []byte) error {
	var url string
	c, err := InitRequestConfig("conf.json")
	if err != nil {
		fmt.Println("===== read conf.json err:", err)
	}
	if c.RecordUrl == "" {
		url = defaultRequestURL
	} else {
		url = c.RecordUrl
	}

	reader := bytes.NewReader(data)
	resp, err := http.Post(url, "application/json", reader)
	if err != nil {
		fmt.Println("===== send post err:", err)
		log.Error("send post err:", err)
		return err
	}
	fmt.Printf("===== resp.Status:%+v, resp.Body:%+v\n", resp.Status, resp.Body)
	defer resp.Body.Close()
	return nil
}

func Load() ([]IncompleteTask, error) {
	var url string
	var iT = make([]IncompleteTask, 0)
	c, err := InitRequestConfig("conf.json")
	if err != nil {
		fmt.Println("===== read conf.json err:", err)
		return iT, err
	}
	if c.RecordUrl == "" {
		url = defaultRequestURL
	} else {
		url = c.RecordUrl
	}

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("===== http get err:", err)
		return iT, err
	}
	defer resp.Body.Close()

	resByte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("===== read all err:", err)
		return iT, err
	}

	err = json.Unmarshal(resByte, &iT)
	if err != nil {
		fmt.Println("===== json Unmarshal err:", err)
		return iT, err
	}

	return iT, nil
}

type Config struct {
	RecordUrl string
	RedisUrl  []string
	PassWord  string
}

func InitRequestConfig(filePath string) (*Config, error) {
	var confInfo = new(Config)
	if !isFileExist(filePath) {
		fn, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		defer fn.Close()
		encoder := json.NewEncoder(fn)
		err = encoder.Encode(confInfo)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	err = decoder.Decode(confInfo)
	if err != nil {
		return nil, err
	}

	return confInfo, nil
}

func isFileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return true
}
