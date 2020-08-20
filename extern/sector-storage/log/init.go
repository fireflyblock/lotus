package log

import (
	"os"
	"sync"
)

func Init() {
	RusPlus = New()
	StdLogger.SetLevel(Level)
	logsDir := "./logs/"

	_, err := os.Stat(logsDir)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(logsDir, 0755)
			if err != nil {
				panic(err)
			}
		}
	}

	DefaultLogger = RusPlus.Logger(logsDir+"default", MaxFileSize, DefaultMaxFiles, Level)
	SchedLogger = RusPlus.Logger(logsDir+"sched", SchedFileSize, SchedMaxFiles, Level)

	Recorder = TimeRecorder{m: sync.Map{}}
	//InitElk(logsDir)
}
