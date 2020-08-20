// +build !release

package log

import (
	"github.com/sirupsen/logrus"
)

var RusPlus *Logrusplus

var StdLogger = logrus.StandardLogger()

var DefaultLogger = logrus.StandardLogger()
var SchedLogger = logrus.StandardLogger()

const (
	MaxFileSize     = 1024 * 1024 * 1024 * 1
	SchedFileSize   = 1024
	DefaultMaxFiles = 5
	SchedMaxFiles   = 100 //core log should keep 12 files to ensure 3 day's logs
	Level           = logrus.DebugLevel
)
