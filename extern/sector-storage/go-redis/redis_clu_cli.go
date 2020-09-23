package goredis

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	logrus "github.com/filecoin-project/sector-storage/log"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
	"time"
)

type RedisClient struct {
	ClusterClient *redis.ClusterClient
	ClientMaster1 *redis.Client
	ClientMaster2 *redis.Client
	ClientMaster3 *redis.Client
	Ctx           context.Context
	TcfRcLK       sync.Mutex //lock TaskConfig
	TctRcLK       sync.Mutex //lock TaskCount
	PubRcLK       sync.Mutex
}

func NewRedisClusterCLi(ctx context.Context, addrs []string, passWord string) *RedisClient {
	log.SetFlags(log.Llongfile | log.Lshortfile)

	// 连接redis集群
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		Password:     passWord,        // 设置密码
		DialTimeout:  1 * time.Second, // 设置连接超时
		ReadTimeout:  1 * time.Second, // 设置读取超时
		WriteTimeout: 1 * time.Second, // 设置写入超时
	})

	if len(addrs) != 3 {
		logrus.SchedLogger.Error("insufficient number of address")
		return nil
	}

	cm1 := redis.NewClient(&redis.Options{Addr: addrs[0], Password: passWord})
	cm2 := redis.NewClient(&redis.Options{Addr: addrs[1], Password: passWord})
	cm3 := redis.NewClient(&redis.Options{Addr: addrs[2], Password: passWord})

	// 发送一个ping命令,测试是否通
	pong, err := clusterClient.Ping(ctx).Result()
	fmt.Println("pong :", pong)
	if err != nil {
		log.Println("ping redis err:", err)
	}

	return &RedisClient{ClusterClient: clusterClient, ClientMaster1: cm1, ClientMaster2: cm2, ClientMaster3: cm3, Ctx: ctx, TcfRcLK: sync.Mutex{}, TctRcLK: sync.Mutex{}, PubRcLK: sync.Mutex{}}
}

// 模糊查询
func (rc *RedisClient) Keys(pattern RedisKey) ([]RedisKey, error) {
	newSlice := make([]RedisKey, 0)
	res, err := rc.ClusterClient.Keys(rc.Ctx, string(pattern)).Result()
	if err != nil {
		return nil, err
	}
	for _, v := range res {
		newSlice = append(newSlice, RedisKey(v))
	}
	return newSlice, nil
}

// 类型添加, v 可以是任意类型
func (rc *RedisClient) Set(key RedisKey, v interface{}, expiration time.Duration) error {
	s, _ := Serialization(v) // 序列化
	_, err := rc.ClusterClient.Set(rc.Ctx, string(key), s, expiration).Result()
	if err != nil {
		return err
	}
	return nil
}

// 获取 任意类型的值，v需要传指针
func (rc *RedisClient) Get(key RedisKey, v interface{}) error {
	temp, err := rc.ClusterClient.Get(rc.Ctx, string(key)).Bytes()
	if err != nil {
		return err
	}
	err = Deserialization(temp, &v) // 反序列化
	if err != nil {
		return err
	}
	return nil
}

// 设置单个值, value 还可以是一个 map slice 等
func (rc *RedisClient) HSet(key RedisKey, field RedisField, value interface{}) error {
	v, _ := Serialization(value)
	_, err := rc.ClusterClient.HSet(rc.Ctx, string(key), string(field), v).Result()
	if err != nil {
		return err
	}
	return nil
}

// 获取单个hash 中的值  params：KEY，获取类型
func (rc *RedisClient) HGet(key RedisKey, field RedisField, v interface{}) error {
	res := rc.ClusterClient.HGet(rc.Ctx, string(key), string(field))
	temp, err := res.Bytes()
	if err != nil {
		return err
	}
	err = Deserialization(temp, &v) // 反序列化
	if err != nil {
		return err
	}
	return nil
}

//发布消息到指定通道  params：通道，消息  return:订阅的消费者数量 error
func (rc *RedisClient) Publish(channel string, message RedisField) (int64, error) {
	reply, err := rc.ClusterClient.Publish(rc.Ctx, channel, string(message)).Result()
	if err != nil {
		return 0, err
	}

	return reply, nil
}

//订阅消息  params：通道
func (rc *RedisClient) Subscribe(channel string) (<-chan *redis.Message, error) {
	sub := rc.ClusterClient.Subscribe(rc.Ctx, channel)
	iface, err := sub.Receive(rc.Ctx)
	if err != nil {
		return nil, err
	}

	//for {
	//	switch v := iface.(type) {
	//	case redis.Subscription:
	//		fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
	//		//return v.Count, nil
	//	case redis.Message:
	//		fmt.Printf("%s: message: %s pattern:%+v PayloadSlice:%+v \n", v.Channel, v.Payload, v.Pattern,v.PayloadSlice)
	//		//return v.Payload, nil
	//	case redis.Pong:
	//		fmt.Printf("%s\n", v.Payload)
	//	case error:
	//		return nil, v
	//	}
	//}

	switch v := iface.(type) {
	case redis.Subscription:
		// subscribe succeeded
		fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
	case redis.Message:
		// received first message
		fmt.Printf("%s: message: %s pattern:%+v PayloadSlice:%+v \n", v.Channel, v.Payload, v.Pattern, v.PayloadSlice)
	case redis.Pong:
		// pong received
		fmt.Printf("%s\n", v.Payload)
	case error:
		// handle error
		return nil, v
	}

	return sub.Channel(), nil

	//for msg := range ch {
	//	fmt.Printf("Cha %+v 接收到 msg %+v", msg.Channel, msg.Payload)
	//	return msg.Payload, nil
	//}
	//return nil, nil
}

//seal的多重addpiece自增id
func (rc *RedisClient) IncrSealAPID(sectorID abi.SectorNumber, value int64) (uint64, error) {
	name := fmt.Sprintf("seal_ap_%d", sectorID)

	id, err := rc.ClusterClient.IncrBy(rc.Ctx, name, value).Result()
	if id < 0 {
		res := id - id
		rc.ClusterClient.IncrBy(rc.Ctx, name, res)
		return 0, errors.New("value already < 0")
	}

	return uint64(id), err
}

//seal的多重addpiece递减id
func (rc *RedisClient) DecrSealAPID(sectorID abi.SectorNumber) (int64, error) {
	name := fmt.Sprintf("seal_ap_%d", sectorID)
	temp, err := rc.ClusterClient.Get(rc.Ctx, name).Int64()
	if err != nil {
		return temp, err
	}

	if temp == 0 {
		return 0, errors.New("seal-ap-id already 0")
	}

	temp--
	s, _ := Serialization(temp) // 序列化
	_, err = rc.ClusterClient.Set(rc.Ctx, name, s, 0).Result()
	if err != nil {
		return temp + 1, err
	}

	return temp, nil
}

func (rc *RedisClient) Incr(key RedisKey, field RedisField, incr int64) (int64, error) {
	return rc.ClusterClient.HIncrBy(rc.Ctx, string(key), string(field), incr).Result()
	//return rc.ClusterClient.Incr(rc.Ctx, string(field)).Result()
}

func (rc *RedisClient) Exist(key RedisKey) (int64, error) {
	return rc.ClusterClient.Exists(rc.Ctx, string(key)).Result()
}

func (rc *RedisClient) HExist(key RedisKey, field RedisField) (bool, error) {
	return rc.ClusterClient.HExists(rc.Ctx, string(key), string(field)).Result()
}
