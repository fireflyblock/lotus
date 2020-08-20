package log

import (
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func Test_Main(t *testing.T) {
	Init()
	stdLogger := logrus.StandardLogger()
	commonLogger := DefaultLogger //lrp.Logger("common")
	schedLogger := SchedLogger

	count := 0
	for {
		go func() {
			schedLogger.WithFields(logrus.Fields{
				"test":  "sch",
				"count": count,
			}).Info("hello world")

			schedLogger.Info("=========== wang de fa ==========", count)
		}()

		go func() {
			stdLogger.WithFields(logrus.Fields{
				"test":  "std",
				"count": count,
			}).Info("hello world")
		}()

		go func() {
			commonLogger.WithFields(logrus.Fields{
				"test":  "common",
				"count": count,
			}).Info("hello world")
		}()

		count++
		if count == 10000 {
			break
		}
		time.Sleep(100) //1 * time.Second)
	}
	clearLogLevel()
}

func clearLogLevel() {
	StdLogger.SetLevel(logrus.PanicLevel)
	SchedLogger.SetLevel(logrus.PanicLevel)
	DefaultLogger.SetLevel(logrus.PanicLevel)
}
