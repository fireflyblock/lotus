package sectorstorage

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	logrus "github.com/filecoin-project/sector-storage/log"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/specs-storage/storage"
	"strconv"
	"strings"
	"time"
)

const CHECK_RES_GAP = time.Minute * 1
const CHECK_RECOVERY_RES_GAP = time.Minute * 5

var (
	//DefaultRedisURL = "192.168.20.178:6379"
	//	"172.16.0.7:8001",
	//	"172.16.0.7:8002",
	//	"172.16.0.8:8001",
	DefaultRedisURL      = ""
	DefaultRedisPassWord = ""
	APWaitTime           = time.Minute * 30
	P1WaitTime           = time.Minute * 210
	P2WaitTime           = time.Minute * 60
	C1WaitTime           = time.Minute * 30
)

func (m *Manager) RecoveryPledge(sectorID abi.SectorNumber, pledgeField gr.RedisField) *gr.ParamsResAp {
	//check pub res
	pubExist, err := m.redisCli.HExist(gr.PARAMS_NAME, pledgeField)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd hexist pledge params err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, pledgeField)
		return nil
	}

	if !pubExist {
		return nil
	}

	defer func() {
		//update taskCount
		//pubExist, err := m.redisCli.HExist(gr.PUB_NAME, pledgeField)
		if err != nil || !pubExist {
			return
		}

		hostName := ""
		err = m.redisCli.HGet(gr.PUB_NAME, pledgeField, &hostName)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hget pledge pub res err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, pledgeField)
			return
		}

		ctk := gr.SplicingTaskCounntKey(hostName)
		field := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPiecePl, 0)
		ctkExist, err := m.redisCli.HExist(ctk, field)
		if err != nil || !ctkExist {
			return
		}

		err = m.FreeTaskCount(hostName, sectorID, sealtasks.TTAddPiecePl, 0)
		if err != nil {
			logrus.SchedLogger.Errorf("===== sector %+v pledge finished , update %s taskCount err:%+v", sectorID, hostName, err)
		}
	}()

	//retry count ++
	err = m.AddRetryCount(pledgeField)
	if err != nil {
		logrus.SchedLogger.Warnf("===== rd add retry count, hget pledge pub err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, pledgeField)
	}

	for {
		//check params res exist
		resExist, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, pledgeField)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hexist pledge params res err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, pledgeField)
			return nil
		}

		if resExist {
			pledgeRes := gr.ParamsResAp{}
			err = m.redisCli.HGet(gr.PARAMS_RES_NAME, pledgeField, &pledgeRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd hget pledge params res err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, pledgeField)
				return nil
			}
			logrus.SchedLogger.Infof("===== rd recovery miner, find sector %d pledgeRes %+v\n", sectorID, pledgeRes)
			return &pledgeRes

		} else {
			logrus.SchedLogger.Infof("===== rd ticker check sectorID %d time %+v", sectorID, time.Now())
			//get time and wait
			var pubTime time.Time
			m.redisCli.HGet(gr.PUB_TIME, pledgeField, &pubTime)
			usedTime := time.Now().Sub(pubTime)
			if usedTime < APWaitTime {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					continue
				case <-time.After(APWaitTime - usedTime):
					m.StoreWaitTime(usedTime, pledgeField)
					continue
				}
			} else {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					m.StoreWaitTime(usedTime, pledgeField)
					continue
				}
			}
		}
	}
}

func (m *Manager) RecoveryP1(sectorID abi.SectorNumber, p1Field gr.RedisField, ticketEpoch abi.ChainEpoch) *gr.ParamsResP1 {
	//check pub res
	pubExist, err := m.redisCli.HExist(gr.PARAMS_NAME, p1Field)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd hexist p1 params err %+v sectorID %+v, p1Field %+v\n", err, sectorID, p1Field)
		return nil
	}

	if !pubExist {
		return nil
	}

	defer func() {
		//update taskCount
		if err != nil || !pubExist {
			return
		}

		hostName := ""
		err = m.redisCli.HGet(gr.PUB_NAME, p1Field, &hostName)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hget p1 pub res err:%+v", err)
		}

		ctk := gr.SplicingTaskCounntKey(hostName)
		exist, err := m.redisCli.HExist(ctk, p1Field)
		if err != nil || !exist {
			return
		}

		err = m.FreeTaskCount(hostName, sectorID, sealtasks.TTPreCommit1, 0)
		if err != nil {
			logrus.SchedLogger.Errorf("===== sector %+v p1 finished , update %s taskCount err:%+v", sectorID, hostName, err)
		}
	}()

	//retry count ++
	err = m.AddRetryCount(p1Field)
	if err != nil {
		logrus.SchedLogger.Warnf("===== rd add retry count, hget p1 pub err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, p1Field)
	}

	for {
		//check params res exist
		resExist, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, p1Field)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hexist p1 params res err %+v sectorID %+v, p1Field %+v\n", err, sectorID, p1Field)
		}
		if resExist {
			p1Res := gr.ParamsResP1{}
			err = m.redisCli.HGet(gr.PARAMS_RES_NAME, p1Field, &p1Res)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd hget p1 params res err:%+v", err)
				return nil
			}
			if p1Res.Err == "" {
				//check ticket
				p1RD := gr.PreCommit1RD{}
				err = m.redisCli.HGet(gr.RECOVER_NAME, p1Field, &p1RD)
				if err != nil {
					logrus.SchedLogger.Errorf("===== rd hget p1 recovery data err %+v sectorID %+v, p1Field %+v\n", err)
					return nil
				}

				if ticketEpoch-p1RD.TicketEpoch < 750 {
					logrus.SchedLogger.Infof("===== rd recovery data ok, sectorID %+v, p1Field %+v\n", sectorID, p1Field)
					return &p1Res
				}
			}
			logrus.SchedLogger.Errorf("===== rd recovery p1 params res err %s sectorID %+v, p1Field %+v\n", p1Res.Err, sectorID, p1Field)
			return nil

		} else {
			//get time and wait
			var pubTime time.Time
			time.Now()
			m.redisCli.HGet(gr.PUB_TIME, p1Field, &pubTime)
			usedTime := time.Now().Sub(pubTime)
			logrus.SchedLogger.Infof("===== rd recovery miner, check p1 sectorID %d, p1Field %s, pubTime %+v, Now %+v, P1WaitTime %+v",
				sectorID, p1Field, pubTime, time.Now(), P1WaitTime)
			if usedTime < P1WaitTime {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					continue
				case <-time.After(P1WaitTime - usedTime):
					m.StoreWaitTime(usedTime, p1Field)
					continue
				}
			} else {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					m.StoreWaitTime(usedTime, p1Field)
					continue
				}
			}
		}
	}
}

func (m *Manager) RecoveryP2(sectorID abi.SectorNumber, p2Field gr.RedisField) *gr.ParamsResP2 {
	//check pub res
	pubExist, err := m.redisCli.HExist(gr.PARAMS_NAME, p2Field)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd hexist p2 params err %+v sectorID %+v, p2Field %+v\n", err, sectorID, p2Field)
		return nil
	}

	if !pubExist {
		return nil
	}

	defer func() {
		//update taskCount
		if err != nil || !pubExist {
			return
		}

		hostName := ""
		err = m.redisCli.HGet(gr.PUB_NAME, p2Field, &hostName)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hget p2 pub res err %+v sectorID %+v, p2Field %+v\n", err, sectorID, p2Field)
			return
		}

		ctk := gr.SplicingTaskCounntKey(hostName)
		field := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTPreCommit2, 0)
		exist, err := m.redisCli.HExist(ctk, field)
		if err != nil || !exist {
			return
		}

		err = m.FreeTaskCount(hostName, sectorID, sealtasks.TTPreCommit2, 0)
		if err != nil {
			logrus.SchedLogger.Errorf("===== sector %+v p2 recovery finished , update %s taskCount err:%+v", sectorID, hostName, err)
		}
	}()

	//retry count ++
	err = m.AddRetryCount(p2Field)
	if err != nil {
		logrus.SchedLogger.Warnf("===== rd add retry count, hget p2 pub err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, p2Field)
	}

	for {
		//check params res exist
		resExist, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, p2Field)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hexist p2 params res err %+v sectorID %+v, p2Field %+v\n", err, sectorID, p2Field)
			return nil
		}
		if resExist {
			p2Res := gr.ParamsResP2{}
			err = m.redisCli.HGet(gr.PARAMS_RES_NAME, p2Field, &p2Res)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd hget p2 params res err %+v sectorID %+v, p2Field %+v\n", err, sectorID, p2Field)
				return nil
			}

			logrus.SchedLogger.Infof("===== rd recovery miner, check p2 sectorID %d, p2Res %+v", sectorID, p2Res)
			return &p2Res

		} else {
			//get time and wait
			var pubTime time.Time
			time.Now()
			m.redisCli.HGet(gr.PUB_TIME, p2Field, &pubTime)
			usedTime := time.Now().Sub(pubTime)
			logrus.SchedLogger.Infof("===== rd recovery miner, check p2 sectorID %d, p2Field %s, pubTime %+v, Now %+v, P2WaitTime %+v",
				sectorID, p2Field, pubTime, time.Now(), P2WaitTime)
			if usedTime < P2WaitTime {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					continue
				case <-time.After(P2WaitTime - usedTime):
					m.StoreWaitTime(usedTime, p2Field)
					continue
				}
			} else {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					m.StoreWaitTime(usedTime, p2Field)
					continue
				}
			}
		}
	}
}

func (m *Manager) RecoveryC1(sectorID abi.SectorNumber, c1Field gr.RedisField) *gr.ParamsResC1 {
	//check pub res
	pubExist, err := m.redisCli.HExist(gr.PARAMS_NAME, c1Field)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd hexist c1 params err %+v sectorID %+v, c1Field %+v\n", err, sectorID, c1Field)
		return nil
	}

	if !pubExist {
		return nil
	}

	defer func() {
		//update taskCount
		if err != nil || !pubExist {
			return
		}

		hostName := ""
		err = m.redisCli.HGet(gr.PUB_NAME, c1Field, &hostName)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hget c1 pub res err %+v sectorID %+v, c1Field %+v\n", err, sectorID, c1Field)
			return
		}

		ctk := gr.SplicingTaskCounntKey(hostName)
		field := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTCommit1, 0)
		exist, err := m.redisCli.HExist(ctk, field)
		if err != nil || !exist {
			return
		}

		err = m.FreeTaskCount(hostName, sectorID, sealtasks.TTCommit1, 0)
		if err != nil {
			logrus.SchedLogger.Errorf("===== sector %+v c1 recovery finished , update %s taskCount err:%+v", sectorID, hostName, err)
		}
	}()

	//retry count ++
	err = m.AddRetryCount(c1Field)
	if err != nil {
		logrus.SchedLogger.Warnf("===== rd add retry count, hget c1 pub err %+v sectorID %+v, pledgeField %+v\n", err, sectorID, c1Field)
	}

	for {
		//check params res exist
		resExist, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, c1Field)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd hexist c1 params res err %+v sectorID %+v, c1Field %+v\n", err, sectorID, c1Field)
			return nil
		}
		if resExist {
			c1Res := gr.ParamsResC1{}
			err = m.redisCli.HGet(gr.PARAMS_RES_NAME, c1Field, &c1Res)
			if err != nil {
				if m.CheckTimeout(err.Error()) {
					logrus.SchedLogger.Errorf("===== rd hget c1 params res timeout %+v sectorID %+v, c1Field %+v\n", err, sectorID, c1Field)
					continue
				}
				logrus.SchedLogger.Errorf("===== rd hget c1 params res err %+v sectorID %+v, c1Field %+v\n", err, sectorID, c1Field)
				return nil
			}

			logrus.SchedLogger.Infof("===== rd recovery miner, check c1 sectorID %d, c1Res %+v", sectorID, c1Res.Err)
			return &c1Res

		} else {
			//get time and wait
			var pubTime time.Time
			m.redisCli.HGet(gr.PUB_TIME, c1Field, &pubTime)
			usedTime := time.Now().Sub(pubTime)
			logrus.SchedLogger.Infof("===== rd recovery miner, check c1 sectorID %d, c1Field %s, pubTime %+v, Now %+v, C1WaitTime %+v",
				sectorID, c1Field, pubTime, time.Now(), C1WaitTime)
			if usedTime < C1WaitTime {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					continue
				case <-time.After(C1WaitTime - usedTime):
					m.StoreWaitTime(usedTime, c1Field)
					continue
				}
			} else {
				select {
				case <-time.After(CHECK_RECOVERY_RES_GAP):
					m.StoreWaitTime(usedTime, c1Field)
					continue
				}
			}
		}
	}
}

func (m *Manager) SubscribeResult(sectorID storage.SectorRef, taskType sealtasks.TaskType, sealApId uint64) (out abi.PieceInfo, err error) {
	ticker := &time.Ticker{}
	switch sectorID.ProofType {
	case abi.RegisteredSealProof_StackedDrg2KiBV1, abi.RegisteredSealProof_StackedDrg2KiBV1_1:
		ticker = time.NewTicker(time.Second)

	case abi.RegisteredSealProof_StackedDrg512MiBV1, abi.RegisteredSealProof_StackedDrg512MiBV1_1:
		ticker = time.NewTicker(time.Second * 10)

	case abi.RegisteredSealProof_StackedDrg32GiBV1, abi.RegisteredSealProof_StackedDrg32GiBV1_1:
		ticker = time.NewTicker(CHECK_RES_GAP)

	default:
		ticker = time.NewTicker(CHECK_RES_GAP)
	}
	defer ticker.Stop()

	resField := gr.SplicingBackupPubAndParamsField(sectorID.ID.Number, taskType, sealApId)
	timer := time.NewTimer(APWaitTime)
	defer timer.Stop()
	start := time.Now()

	for {
		select {
		//case msg := <-subCha:
		//	//check sub
		//	//logrus.SchedLogger.Infof("===== Cha %+v receive msg %+v, sectorID %+v taskType %+v", msg.Channel, msg.Payload, sectorID, taskType)
		//	pl := gr.RedisField(msg.Payload)
		//	sid, tt, hostName, sealID, err := pl.TailoredSubMessage()
		//	if err != nil {
		//		logrus.SchedLogger.Errorf("sub tailored err:", err)
		//	}
		//
		//	if sid == sectorID && tt.ToOfficalTaskType() == taskType && sealID == sealApId {
		//		logrus.SchedLogger.Infof("===== rd subscribe task, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sectorID, taskType)
		//		//get params res
		//		resField := gr.SplicingBackupPubAndParamsField(sectorID, taskType, sealID)
		//
		//		paramsRes := &gr.ParamsResAp{}
		//		err = m.redisCli.HGet(gr.PARAMS_RES_NAME, resField, paramsRes)
		//		if err != nil {
		//			logrus.SchedLogger.Errorf("===== hget ap res err:%+v", err)
		//			return out, err
		//		}
		//
		//		if paramsRes.Err != "" {
		//			logrus.SchedLogger.Errorf("===== sector(%+v) ap computing err:%+v", sectorID, paramsRes.Err)
		//			return out, errors.New(fmt.Sprintf("%d ap res err: %s", sectorID, paramsRes.Err))
		//		}
		//
		//		//update taskCount (need lock)
		//		defer func() {
		//			err = m.FreeTaskCount(hostName, sectorID, taskType, sealApId)
		//			if err != nil {
		//				logrus.SchedLogger.Errorf("===== sector %+v ap finished , update %s taskCount err:%+v", sectorID, hostName, err)
		//			}
		//		}()
		//
		//		return paramsRes.PieceInfo, nil
		//	} else {
		//		continue
		//	}

		case <-timer.C: //12600
			m.StoreWaitTime(APWaitTime, resField)
			continue

		case <-ticker.C:
			usedTime := time.Now().Sub(start)
			if usedTime > APWaitTime {
				m.StoreWaitTime(usedTime, resField)
			}
			hostName := ""
			//check params
			exist, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, resField)
			if err != nil {
				logrus.SchedLogger.Errorf("===== HExist ap res params err:%+v", err)
				continue
			}

			if !exist {
				continue
			}

			//get res
			err = m.redisCli.HGet(gr.PUB_RES_NAME, resField, &hostName)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget ap res err:%+v", err)
				continue
			}

			logrus.SchedLogger.Infof("===== rd ticker check task, sectorID %+v taskType %+v worker %+v\n", sectorID, taskType, hostName)
			//get params res
			paramsRes := &gr.ParamsResAp{}
			err = m.redisCli.HGet(gr.PARAMS_RES_NAME, resField, paramsRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget ap res params err:%+v", err)
				continue
			}

			if paramsRes.Err != "" {
				logrus.SchedLogger.Errorf("===== sector(%+v) ap computing err:%+v", sectorID, paramsRes.Err)
				return out, errors.New(fmt.Sprintf("%d ap res err: %s", sectorID, paramsRes.Err))
			}

			//update taskCount (need lock)
			defer func() {
				err = m.FreeTaskCount(hostName, sectorID.ID.Number, taskType, sealApId)
				if err != nil {
					logrus.SchedLogger.Errorf("===== sector %+v ap finished , update %s taskCount err:%+v", sectorID, hostName, err)
				}
			}()

			return paramsRes.PieceInfo, nil
		}
	}
}

func (m *Manager) PublishTask(sectorID storage.SectorRef, taskType sealtasks.TaskType, params []byte, sealAPID uint64) (uint64, error) {
	var publishCha string
	switch taskType {
	case sealtasks.TTAddPiecePl, sealtasks.TTAddPieceSe:
		m.redisCli.ApRcLK.Lock()
		logrus.SchedLogger.Infof("===== rd lock1 taskType %+v sectorID %+v", taskType, sectorID)
		defer func() {
			m.redisCli.ApRcLK.Unlock()
			logrus.SchedLogger.Infof("===== rd lock2 taskType %+v sectorID %+v", taskType, sectorID)
		}()
		publishCha = gr.PUBLISHCHANNELAP

	case sealtasks.TTPreCommit1:
		m.redisCli.P1RcLK.Lock()
		defer m.redisCli.P1RcLK.Unlock()
		publishCha = gr.PUBLISHCHANNELP1

	case sealtasks.TTPreCommit2:
		m.redisCli.P2RcLK.Lock()
		defer m.redisCli.P2RcLK.Unlock()
		publishCha = gr.PUBLISHCHANNELP2

	case sealtasks.TTCommit1:
		m.redisCli.C1RcLK.Lock()
		defer m.redisCli.C1RcLK.Unlock()
		publishCha = gr.PUBLISHCHANNELC1
	}

	//1.1 select worker
SEACHAGAIN:
	hostName, err := m.SelectWorker(sectorID.ID.Number, taskType)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd select worker failed, sectorID %+v taskType %+v select worker err: %+v", sectorID, taskType, err)
		return sealAPID, err
	}
	if hostName == "" {
		switch taskType {
		case sealtasks.TTAddPieceSe, sealtasks.TTPreCommit2, sealtasks.TTCommit1:
			if taskType == sealtasks.TTAddPieceSe {
				m.isExistSealTask = false
			}
			logrus.SchedLogger.Infof("===== rd sub free worker signal, sectorID %+v taskType %+v isExistSealTask %+v", sectorID, taskType, m.isExistSealTask)
			//subCha, err := m.redisCli.Subscribe(gr.SUBSCRIBECHANNEL)
			//if err != nil {
			//	goto SEACHAGAIN
			//}
			hostName, err = m.SubscribeFreeWorker(taskType, sectorID)
			if err != nil || hostName == "" {
				goto SEACHAGAIN
			}
			if taskType == sealtasks.TTAddPieceSe {
				m.isExistSealTask = true
			}

		case sealtasks.TTAddPiecePl, sealtasks.TTPreCommit1:
			logrus.SchedLogger.Infof("===== rd no free worker, sectorID %+v taskType %+v isExistSealTask %+v", sectorID, taskType, m.isExistSealTask)
			return sealAPID, errors.New("No free workers")
		}
	}

	logrus.SchedLogger.Infof("===== rd select worker success hostname %+v sectorID %+v taskType %+v", hostName, sectorID, taskType)
	//update sealAPID
	if taskType == sealtasks.TTAddPieceSe {
		sealAPID, err = m.redisCli.IncrSealAPID(sectorID.ID.Number, 1)
	}

	pubField := gr.SplicingBackupPubAndParamsField(sectorID.ID.Number, taskType, sealAPID)
	//1.2 backup params
	//logrus.SchedLogger.Infof("===== rd backup params ")
	err = m.redisCli.HSet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		return sealAPID, err
	}

	//1.3 backup  pub
	//logrus.SchedLogger.Infof("===== rd backup pub")
	err = m.redisCli.HSet(gr.PUB_NAME, pubField, hostName)
	if err != nil {
		return sealAPID, errors.New("backup pub err:" + err.Error())
	}

	//1.4 store pub time
	err = m.redisCli.HSet(gr.PUB_TIME, pubField, time.Now())
	if err != nil {
		return sealAPID, err
	}

	//1.5 update status
	err = m.UpdateStatus(sectorID.ID.Number, taskType, hostName)
	if err != nil {
		log.Errorf("===== rd update status err", err)
	}

	//1.6 publish task
	//logrus.SchedLogger.Infof("===== rd start publish")
	pubMsg := gr.SplicingPubMessage(sectorID.ID.Number, taskType, hostName, sealAPID)
	_, err = m.redisCli.Publish(publishCha, pubMsg)
	logrus.SchedLogger.Infof("===== rd publish cha %s msg %+v, sectorID %+v taskType %+v err %+v", publishCha, pubMsg, sectorID, taskType, err)
	if err != nil {
		return sealAPID, err
	}

	//if it si addpiece of seal >1,return directly  witnout updating taskCount
	if sealAPID > 1 {
		return sealAPID, nil
	}

	//1.7 update taskCount (need lock)
	err = m.AddTaskCount(hostName, sectorID.ID.Number, taskType, sealAPID)
	if err != nil {
		return sealAPID, err
	}
	//1.8 update p1Counter
	if taskType == sealtasks.TTPreCommit1 {
		err = m.AddP1Count(hostName, sectorID.ID.Number, taskType, sealAPID)
		if err != nil {
			return sealAPID, err
		}
	}

	return sealAPID, nil
}

func (m *Manager) SubscribeFreeWorker(taskType sealtasks.TaskType, sectorID storage.SectorRef) (hostName string, err error) {
	ticker := &time.Ticker{}
	switch taskType {
	case sealtasks.TTAddPieceSe:
		m.redisCli.ApRcLK.Unlock()
		logrus.SchedLogger.Infof("===== rd lock3 taskType %+v sectorID %+v", taskType, sectorID)

	case sealtasks.TTPreCommit1:
		m.redisCli.P1RcLK.Unlock()

	case sealtasks.TTPreCommit2:
		m.redisCli.P2RcLK.Unlock()

	case sealtasks.TTCommit1:
		m.redisCli.C1RcLK.Unlock()
	}
	defer ticker.Stop()

	logrus.SchedLogger.Infof("===== rd start subscribe free worker, sectorID %+v taskType %+v", sectorID, taskType)

	switch sectorID.ProofType {
	case abi.RegisteredSealProof_StackedDrg2KiBV1:
		ticker = time.NewTicker(time.Minute)

	case abi.RegisteredSealProof_StackedDrg512MiBV1:
		ticker = time.NewTicker(time.Minute)

	case abi.RegisteredSealProof_StackedDrg32GiBV1:
		ticker = time.NewTicker(CHECK_RES_GAP)

	default:
		ticker = time.NewTicker(CHECK_RES_GAP)
	}

	for {
		select {
		//case msg := <-subCha:
		//	//logrus.SchedLogger.Infof("===== Cha %+v receive msg %+v", msg.Channel, msg.Payload)
		//	pl := gr.RedisField(msg.Payload)
		//	sid, tt, hostName, _, err := pl.TailoredSubMessage()
		//	if err != nil {
		//		logrus.SchedLogger.Errorf("sub tailored err:", err)
		//	}
		//
		//	switch taskType {
		//	case sealtasks.TTAddPieceSe:
		//		if tt.ToOfficalTaskType() == taskType || tt.ToOfficalTaskType() == sealtasks.TTAddPiecePl {
		//			m.SelectLock(taskType, sectorID)
		//			free, _ := m.CanHandleTask(hostName, taskType, sectorID)
		//			if free {
		//				logrus.SchedLogger.Infof("===== rd subscribe free worker, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sectorID, taskType)
		//				return hostName, nil
		//			} else {
		//				m.SelectUnLock(taskType, sectorID)
		//				continue
		//			}
		//		} else {
		//			//m.SelectUnLock(taskType,sectorID)
		//			continue
		//		}
		//
		//	case sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1:
		//		if tt.ToOfficalTaskType() == taskType && sid == sectorID {
		//			m.SelectLock(taskType, sectorID)
		//			free, _ := m.CanHandleTask(hostName, taskType, sectorID)
		//			if free {
		//				logrus.SchedLogger.Infof("===== rd subscribe free worker, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sectorID, taskType)
		//				return hostName, nil
		//			} else {
		//				m.SelectUnLock(taskType, sectorID)
		//				continue
		//			}
		//		} else {
		//			//m.SelectUnLock(taskType,sectorID)
		//			continue
		//		}
		//	}

		case <-ticker.C:
			m.SelectLock(taskType, sectorID.ID.Number)
			logrus.SchedLogger.Infof("===== rd ticker free worker, sectorID %+v taskType %+v", sectorID, taskType)
			switch taskType {
			case sealtasks.TTAddPieceSe:
				hostName, err = m.SeachWorker(gr.ToFieldTaskType(taskType), sectorID.ID.Number)
				if err != nil {
					m.SelectUnLock(taskType, sectorID.ID.Number)
					continue
				}
				if hostName == "" {
					m.SelectUnLock(taskType, sectorID.ID.Number)
					continue
				}
				return hostName, nil

			case sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1:
				hostName, err = m.BindWorker(sectorID.ID.Number, taskType)
				if err != nil {
					m.SelectUnLock(taskType, sectorID.ID.Number)
					continue
				}
				if hostName == "" {
					m.SelectUnLock(taskType, sectorID.ID.Number)
					continue
				}
				return hostName, nil
			}
		}
	}
}

func (m *Manager) SelectLock(taskType sealtasks.TaskType, sectorID abi.SectorNumber) {
	switch taskType {
	case sealtasks.TTAddPieceSe:
		m.redisCli.ApRcLK.Lock()

	case sealtasks.TTPreCommit1:
		m.redisCli.P1RcLK.Lock()

	case sealtasks.TTPreCommit2:
		m.redisCli.P2RcLK.Lock()

	case sealtasks.TTCommit1:
		m.redisCli.C1RcLK.Lock()
	}
	logrus.SchedLogger.Infof("===== rd lock taskType %+v sectorID %+v", taskType, sectorID)
}

func (m *Manager) SelectUnLock(taskType sealtasks.TaskType, sectorID abi.SectorNumber) {
	switch taskType {
	case sealtasks.TTAddPieceSe:
		m.redisCli.ApRcLK.Unlock()

	case sealtasks.TTPreCommit1:
		m.redisCli.P1RcLK.Unlock()

	case sealtasks.TTPreCommit2:
		m.redisCli.P2RcLK.Unlock()

	case sealtasks.TTCommit1:
		m.redisCli.C1RcLK.Unlock()
	}
	logrus.SchedLogger.Infof("===== rd unlock taskType %+v sectorID %+v", taskType, sectorID)
}

func (m *Manager) SelectWorker(sectorID abi.SectorNumber, taskType sealtasks.TaskType) (hostName string, err error) {
	//1.1 select worker
	switch taskType {
	case sealtasks.TTAddPieceSe:
		pubFieldSe := gr.SplicingBackupPubAndParamsField(sectorID, taskType, 1)
		exist, err := m.redisCli.HExist(gr.PUB_NAME, pubFieldSe)
		if err != nil {
			return "", err
		}
		if !exist {
			hostName, err = m.SeachWorker(gr.ToFieldTaskType(taskType), sectorID)
			if err != nil {
				return "", err
			}
			return hostName, nil

		} else {
			hostName, err = m.BindWorker(sectorID, taskType)
			if err != nil {
				return "", err
			}
			return hostName, nil
		}

	case sealtasks.TTAddPiecePl:
		if !m.isExistSealTask {
			return hostName, errors.New("worker is full of tasks and there is a seal task waiting")
		}
		hostName, err := m.SeachWorker(gr.ToFieldTaskType(taskType), sectorID)
		if err != nil {
			return "", err
		}

		if hostName == "" {
			return hostName, errors.New("worker is full of tasks")
		}

		return hostName, nil

	case sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1:
		//logrus.SchedLogger.Infof("===== rd ppc select worker, sectorID%+v, taskType:%+v", sectorID, taskType)
		hostName, err := m.BindWorker(sectorID, taskType)
		if err != nil {
			return "", err
		}

		return hostName, nil
	default:
		return "", errors.New("Unkown task type")
	}
}

func (m *Manager) SeachWorker(taskType gr.RedisField, sectorID abi.SectorNumber) (hostName string, err error) {
	//m.redisCli.TcfRcLK.Lock()
	//defer m.redisCli.TcfRcLK.Unlock()
	workerList, err := m.redisCli.Keys(gr.WORKER_CONFIG)

	//logrus.SchedLogger.Infof("===== rd SeachW  :%+v", workerList)
	for _, v := range workerList {
		hostName = v.TailoredWorker()
		free, err := m.CanHandleTask(hostName, taskType.ToOfficalTaskType(), sectorID)
		if err != nil {
			return "", err
		}
		if free {
			return hostName, nil
		} else {
			continue
		}
	}
	return "", nil //errors.New("hostname is not exist")
}

func (m *Manager) BindWorker(sectorID abi.SectorNumber, taskType sealtasks.TaskType) (hostName string, err error) {
	pubFieldSe := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, 1)
	exist, err := m.redisCli.HExist(gr.PUB_NAME, pubFieldSe)
	if err != nil {
		return "", err
	}
	var pubFieldPl gr.RedisField
	if !exist {
		pubFieldPl = gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPiecePl, 0)
	} else {
		pubFieldPl = gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, 1)
	}

	err = m.redisCli.HGet(gr.PUB_NAME, pubFieldPl, &hostName)
	if err != nil {
		return "", err
	}
	//if it si addpiece of seal >1,return directly  witnout judging taskCount
	if taskType == sealtasks.TTAddPieceSe {
		return hostName, nil
	}

	free, err := m.CanHandleTask(hostName, taskType, sectorID)
	if err != nil {
		return "", err
	}
	if free {
		return hostName, nil
	} else {
		return "", nil //errors.New("worker is full of tasks")
	}
}

func (m *Manager) CanHandleTask(hostname string, taskType sealtasks.TaskType, sectorID abi.SectorNumber) (free bool, err error) {
	var (
		tfk             = gr.SplicingTaskConfigKey(hostname)
		ttk             = gr.SplicingTaskCounntKey(hostname)
		TaskConfigField sealtasks.TaskType
		TaskCountField  sealtasks.TaskType
		numberCt        uint64
		plCount         uint64
		p1Count         uint64
		p2Count         uint64
		c1Count         uint64
		seCount         = make(map[abi.SectorNumber]struct{}, 0)
	)

	//get config
	var numberCf uint64
	if taskType == sealtasks.TTAddPieceSe || taskType == sealtasks.TTAddPiecePl {
		TaskConfigField = sealtasks.TTAddPiece
		TaskCountField = taskType
	} else {
		TaskConfigField = taskType
		TaskCountField = taskType
	}

	//logrus.SchedLogger.Infof("===== rd CanHandleTask, hget worker %+v taskType1 :%+v taskType2 %+v", tfk, gr.ToFieldTaskType(TaskConfigField), taskType)
	err = m.redisCli.HGet(tfk, gr.ToFieldTaskType(TaskConfigField), &numberCf)
	if err != nil {
		logrus.SchedLogger.Infof("===== rd canHT err %+v worker %+v config :%+v taskType %+v", err, hostname, numberCf, taskType)
		return free, err
	}
	if numberCf == 0 {
		return free, err
	}

	//check p1 counter
	if taskType == sealtasks.TTAddPiecePl || taskType == sealtasks.TTAddPieceSe || taskType == sealtasks.TTPreCommit1 {
		cp1Exist := m.CheckP1Counter(hostname)
		if !cp1Exist {
			return free, nil
		}
	}

	//check p1waiting
	if taskType == sealtasks.TTAddPiecePl || taskType == sealtasks.TTAddPieceSe {
		var p1Cf uint64
		err = m.redisCli.HGet(tfk, gr.ToFieldTaskType(sealtasks.TTPreCommit1), &p1Cf)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd CalculateWaitingTask err %+v worker %+v config :%+v ", err, hostname, p1Cf)
		}

		res := m.CalculateWaitingTask(hostname, taskType)
		if len(res) >= int(p1Cf) {
			logrus.SchedLogger.Infof("===== rd check worker %+v p1waiting count %d limit %d", hostname, len(res), p1Cf)
			return free, nil
		}
	}

	//get count (need lock)
	count, err := m.redisCli.Exist(ttk)
	if count == 0 {
		return true, err
	}

	list, err := m.redisCli.HKeys(ttk)
	if err != nil {
		logrus.SchedLogger.Infof("===== rd canHT err %+v worker %+v sectorID :%+v taskType %+v", err, hostname, sectorID, taskType)
		return free, err
	}

	for _, v := range list {
		sid, tt, _, _ := v.TailoredPubAndParamsfield()
		if sid == sectorID && gr.RedisField(tt).ToOfficalTaskType() == taskType {
			return true, nil
		}

		if gr.RedisField(tt).ToOfficalTaskType() == TaskCountField {
			numberCt++
		}

		switch tt {
		case gr.FIELDPLEDGEP.ToString():
			plCount++

		case gr.FIELDSEAL.ToString():
			seCount[sid] = struct{}{}

		case gr.FIELDP1.ToString():
			p1Count++

		case gr.FIELDP2.ToString():
			p2Count++

		case gr.FIELDC1.ToString():
			c1Count++
		}
	}
	logrus.SchedLogger.Infof("===== rd canHT worker %+v sectorID %+v taskType %+v : "+
		"[ pledge %d seal %d p1 %d p2 %d c1 %d ]", hostname, sectorID, taskType,
		plCount, len(seCount), p1Count, p2Count, c1Count)

	//compare
	if numberCt > numberCf {
		logrus.SchedLogger.Errorf("===== rd compare err, config:%d count:%d , worker %+v sectorID %+v taskType %+v, ", numberCf, numberCt, hostname, sectorID, taskType)
	}
	//compare
	if numberCf > numberCt {
		return true, nil
	} else {
		return free, nil
	}
}

func (m *Manager) CheckP1Counter(hostname string) (free bool) {
	var p1Cf uint64
	//p1Field := gr.SplicingBackupPubAndParamsField(sectorID, taskType, 0)
	tfk := gr.SplicingTaskConfigKey(hostname)
	err := m.redisCli.HGet(tfk, gr.ToFieldTaskType(sealtasks.TTPreCommit1), &p1Cf)
	if err != nil {
		logrus.SchedLogger.Infof("===== rd CheckP1Counter err %+v worker %+v config :%+v ", err, hostname, p1Cf)
	}

	cp1 := gr.SplicingCounterP1Key(hostname)
	cp1List, err := m.redisCli.HKeys(cp1)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd CheckP1Counter err %+v worker %+v ", err, hostname)
	}
	maxCount := p1Cf*2 + 3
	logrus.SchedLogger.Infof("===== rd CheckP1Counter worker %+v cp1 %d maxCount %d", hostname, len(cp1List), maxCount)
	if len(cp1List) >= int(maxCount) {
		return free
	}
	return true
}

func (m *Manager) FreeTaskCount(hostName string, sectorID abi.SectorNumber, taskType sealtasks.TaskType, sealApId uint64) error {
	//switch taskType {
	//case sealtasks.TTAddPiecePl, sealtasks.TTAddPieceSe, sealtasks.TTAddPiece:
	//	if taskType == sealtasks.TTAddPieceSe && sealApId > 1 {
	//		return nil
	//	}
	//	m.redisCli.ApRcLK.Lock()
	//	logrus.SchedLogger.Infof("===== rd lock4 taskType %+v sectorID %+v", taskType, sectorID)
	//
	//	defer func() {
	//		m.redisCli.ApRcLK.Unlock()
	//		logrus.SchedLogger.Infof("===== rd lock5 taskType %+v sectorID %+v", taskType, sectorID)
	//	}()
	//
	//case sealtasks.TTPreCommit1:
	//	m.redisCli.P1RcLK.Lock()
	//	defer m.redisCli.P1RcLK.Unlock()
	//
	//case sealtasks.TTPreCommit2:
	//	m.redisCli.P2RcLK.Lock()
	//	defer m.redisCli.P2RcLK.Unlock()
	//
	//case sealtasks.TTCommit1:
	//	m.redisCli.C1RcLK.Lock()
	//	defer m.redisCli.C1RcLK.Unlock()
	//}

	ctk := gr.SplicingTaskCounntKey(hostName)
	field := gr.SplicingBackupPubAndParamsField(sectorID, taskType, sealApId)
	_, err := m.redisCli.HDel(ctk, field)
	logrus.SchedLogger.Infof("===== rd free workerCount hostname %+v sectorID %+v taskType %+v field %+v", hostName, sectorID, taskType, field)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) AddTaskCount(hostName string, sectorID abi.SectorNumber, taskType sealtasks.TaskType, sealApId uint64) error {
	ctk := gr.SplicingTaskCounntKey(hostName)
	field := gr.SplicingBackupPubAndParamsField(sectorID, taskType, sealApId)
	_, err := m.redisCli.HIncr(ctk, field, 1)
	logrus.SchedLogger.Infof("===== rd add workerCount hostname %+v sectorID %+v taskType %+v ctk %+v field %+v", hostName, sectorID, taskType, ctk, field)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd add workerCount ctk %+v field %+v err %+v", ctk, field, err)
		return err
	}
	return nil
}

func (m *Manager) AddP1Count(hostName string, sectorID abi.SectorNumber, taskType sealtasks.TaskType, sealApId uint64) error {
	cp1 := gr.SplicingCounterP1Key(hostName)
	field := gr.SplicingBackupPubAndParamsField(sectorID, taskType, sealApId)
	_, err := m.redisCli.HIncr(cp1, field, 1)
	logrus.SchedLogger.Infof("===== rd add COUNTER_P1 hostname %+v sectorID %+v taskType %+v cp1 %+v field %+v", hostName, sectorID, taskType, cp1, field)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd add COUNTER_P1 cp1 %+v field %+v err %+v", cp1, field, err)
		return err
	}
	return nil
}

func (m *Manager) FreeP1Count(hostName string, sectorID abi.SectorNumber, taskType sealtasks.TaskType, sealApId uint64) error {
	cp1 := gr.SplicingCounterP1Key(hostName)
	field := gr.SplicingBackupPubAndParamsField(sectorID, taskType, sealApId)
	_, err := m.redisCli.HDel(cp1, field)
	logrus.SchedLogger.Infof("===== rd free COUNTER_P1 hostname %+v sectorID %+v taskType %+v field %+v", hostName, sectorID, taskType, field)
	if err != nil {
		return err
	}
	return nil
}

func (m *Manager) DeleteDataForSid(sectorID abi.SectorNumber) {
	logrus.SchedLogger.Infof("===== rd DeleteDataForSid, sectorID %+v", sectorID)
	sealKey := gr.RedisKey(fmt.Sprintf("seal_ap_%d", sectorID))
	tasklist := make([]sealtasks.TaskType, 0)
	hostname := ""

	res, err := m.redisCli.Exist(sealKey)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}
	if res > 0 {
		list := []sealtasks.TaskType{sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		tasklist = append(tasklist, list...)
	} else {

		list := []sealtasks.TaskType{sealtasks.TTAddPiecePl, sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		tasklist = append(tasklist, list...)
	}

	for _, v := range tasklist {
		f := gr.SplicingBackupPubAndParamsField(sectorID, v, 0)
		//1 pub
		m.redisCli.HDel(gr.PARAMS_NAME, f)
		//2 params
		m.redisCli.HDel(gr.PUB_NAME, f)
		//3 res
		m.redisCli.HDel(gr.PUB_RES_NAME, f)
		//4 res params
		m.redisCli.HDel(gr.PARAMS_RES_NAME, f)
		//5 pub time
		m.redisCli.HDel(gr.PUB_TIME, f)
		if v == sealtasks.TTPreCommit1 {
			//6 recovery
			m.redisCli.HDel(gr.RECOVER_NAME, f)
		}
		//7 wait time
		m.redisCli.HDel(gr.RECOVERY_WAIT_TIME, f)
		//8 task status
		ex, err := m.redisCli.HExist(gr.PUB_NAME, f)
		if err != nil {
			log.Errorf("===== rd HExist hostname for DeleteDataForSid, sectorID %+v err %+v", sectorID, err)
			continue
		}
		if ex {
			err = m.redisCli.HGet(gr.PUB_NAME, f, &hostname)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd DeleteDataForSid, hget pledge pub err %+v sectorID %+v, field %+v\n", err, sectorID, f)
			}
			m.redisCli.HDel(gr.RedisKey(hostname), gr.RedisField(sectorID.String()))
			//9 retry count
			m.FreeRetryCount(f, hostname)
		}
	}

	if res == 0 {
		return
	}

	var count int64
	err = m.redisCli.Get(sealKey, &count)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}

	//1 seal key
	m.redisCli.Del(sealKey)

	var i int64 = 1
	for i = 1; i <= count; i++ {
		f := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, uint64(i))
		//2 pub
		m.redisCli.HDel(gr.PARAMS_NAME, f)
		//3 res params
		m.redisCli.HDel(gr.PUB_NAME, f)
		//4 res
		m.redisCli.HDel(gr.PUB_RES_NAME, f)
		//5 res params
		m.redisCli.HDel(gr.PARAMS_RES_NAME, f)
		//6 pub time
		m.redisCli.HDel(gr.PUB_TIME, f)
		//7 retry count
		m.FreeRetryCount(f, hostname)
		//8 wait time
		m.redisCli.HDel(gr.RECOVERY_WAIT_TIME, f)

	}
}

func (m *Manager) DeleteParamsRes(sectorID abi.SectorNumber, tashType sealtasks.TaskType) {
	logrus.SchedLogger.Infof("===== rd DeleteParamsRes, sectorID %+v", sectorID)
	f := gr.SplicingBackupPubAndParamsField(sectorID, tashType, 0)

	m.redisCli.HDel(gr.PARAMS_NAME, f)

	m.redisCli.HDel(gr.PUB_NAME, f)

	m.redisCli.HDel(gr.PARAMS_RES_NAME, f)

	m.redisCli.HDel(gr.PUB_RES_NAME, f)

	m.redisCli.HDel(gr.RECOVERY_WAIT_TIME, f)

}

func (m *Manager) UpdateStatus(sectorID abi.SectorNumber, taskType sealtasks.TaskType, hostName string) error {
	return m.redisCli.HSet(gr.RedisKey(hostName), gr.RedisField(strconv.Itoa(int(sectorID))), gr.ToFieldTaskType(taskType))
}

func (m *Manager) CheckTimeout(err string) bool {
	count1 := strings.Count(err, "read tcp")
	count2 := strings.Count(err, "timeout")

	if count1 > 0 && count2 > 0 {
		return true
	}
	return false
}

func (m *Manager) AddRetryCount(retryField gr.RedisField) error {
	sid, tt, _, _ := retryField.TailoredPubAndParamsfield()
	hostname := ""
	err := m.redisCli.HGet(gr.PUB_NAME, retryField, &hostname)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd add retry count hget pledge pub err %+v sectorID %+v, pledgeField %+v\n", err, sid, retryField)
	}

	_, err = m.redisCli.HIncr(gr.RETRY, retryField, 1)
	logrus.SchedLogger.Infof("===== rd add retry count hostname %+v sectorID %+v taskType %+v field %+v", hostname, sid, tt, retryField)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd add retry count field %+v err %+v", retryField, err)
		return err
	}
	return nil
}

func (m *Manager) FreeRetryCount(retryField gr.RedisField, hostname string) error {
	sid, tt, _, _ := retryField.TailoredPubAndParamsfield()
	_, err := m.redisCli.HDel(gr.RETRY, retryField)
	logrus.SchedLogger.Infof("===== rd free retry count, hostname %+v sectorID %+v taskType %+v field %+v", hostname, sid, tt, retryField)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd free retry count field %+v err %+v", retryField, err)
		return err
	}
	return nil
}

func (m *Manager) WhetherToDeleteRetryCount(retryField gr.RedisField, hostname string) error {
	//exist
	//sid, tt, _, _ := retryField.TailoredPubAndParamsfield()
	ex, err := m.redisCli.HExist(gr.RETRY, retryField)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd free retry count, hexists field %+v err %+v", retryField, err)
		return err
	}
	if !ex {
		return nil
	}

	//>1
	var retryCount int64
	err = m.redisCli.HGet(gr.RETRY, retryField, &retryCount)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd free retry count field, hget %+v err %+v", retryField, err)
		return err
	}
	if retryCount == 1 {
		logrus.SchedLogger.Errorf("===== rd WhetherToDeleteRetryCount field %+v retryCount %+v", retryField, retryCount)
		m.FreeRetryCount(retryField, hostname)
	}
	return nil
}

func (m *Manager) StoreWaitTime(usedTime time.Duration, retryField gr.RedisField) error {
	sid, tt, _, _ := retryField.TailoredPubAndParamsfield()
	hostname := ""
	err := m.redisCli.HGet(gr.PUB_NAME, retryField, &hostname)
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd store wait time, hget pub err %+v sectorID %+v, pledgeField %+v\n", err, sid, retryField)
	}

	err = m.redisCli.HSet(gr.RECOVERY_WAIT_TIME, retryField, usedTime.Seconds())
	logrus.SchedLogger.Infof("===== rd store wait time, hostname %+v sectorID %+v taskType %+v field %+v usedTime %+v", hostname, sid, tt, retryField, usedTime.Seconds())
	if err != nil {
		logrus.SchedLogger.Errorf("===== rd add retry count field %+v usedTime %+v err %+v", retryField, usedTime.Seconds(), err)
		return err
	}
	return nil
}

func (m *Manager) CalculateWaitingTask(hostname string, taskType sealtasks.TaskType) []gr.RedisField {
	pledgeKeys := make([]gr.RedisField, 0)
	p1Keys := make([]gr.RedisField, 0)
	allKeyList, _ := m.redisCli.HKeys(gr.RedisKey(hostname))
	for _, key := range allKeyList {
		var tt gr.RedisField
		m.redisCli.HGet(gr.RedisKey(hostname), key, &tt)
		switch gr.ToFieldTaskType(taskType) {
		case gr.FIELDPLEDGEP:
			field := gr.SplicingBackupPubAndParamsField(key.ToSectorNumber(), tt.ToOfficalTaskType(), 0)
			if tt == gr.FIELDPLEDGEP {
				pledgeKeys = append(pledgeKeys, field)
			}
			if tt == gr.FIELDP1 {
				p1Keys = append(p1Keys, field)
			}
		}
	}

	i := 0
	for j := 0; j < len(pledgeKeys); j++ {
		ex, err := m.redisCli.HExist(gr.PARAMS_RES_NAME, pledgeKeys[j])
		if err != nil {
			log.Errorf("rd search recovery pledge, filter3 err:%+v", err)
		}
		if !ex {
			continue
		}

		pledgeRes := &gr.ParamsResAp{}
		err = m.redisCli.HGet(gr.PARAMS_RES_NAME, pledgeKeys[j], pledgeRes)
		if err != nil {
			log.Errorf("rd search recovery pledge, filter4 err:%+v", err)
		}

		if pledgeRes.Err == "" {
			pledgeKeys[i] = pledgeKeys[j]
			i++
		}
	}

	i = 0
	for j := 0; j < len(p1Keys); j++ {
		ex, err := m.redisCli.HExist(gr.PUB_NAME, p1Keys[j])
		if err != nil {
			log.Errorf("rd search recovery pledge, filter3 err:%+v", err)
		}
		if !ex {
			p1Keys[i] = p1Keys[j]
			i++
		}

	}
	p1Keys = p1Keys[:i]
	for _, v := range p1Keys {
		pledgeKeys = append(pledgeKeys, gr.TypeToType(v, gr.FIELDPLEDGEP))
	}

	logrus.SchedLogger.Infof("===== rd CalculateWaitingTask worker %+v len %+v pledgeKeys %+v", hostname, len(pledgeKeys), pledgeKeys)

	return pledgeKeys
}
