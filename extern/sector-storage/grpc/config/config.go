package config

import (
	"github.com/BurntSushi/toml"
	logging "github.com/ipfs/go-log/v2"
)

type Config struct {
	GRPC struct {
		IP   string `toml:"ip"`
		Port string `toml:"port"`
	}

	LOCALHOST struct {
		Port string `toml:"port"`
	}
}

var C Config
var log = logging.Logger("grpc")

func Init() {
	if _, err := toml.DecodeFile("./config/default.toml", &C); err != nil {
		log.Errorf("Parse config file:default.toml error:%s", err)
		panic(err)
	}
}
