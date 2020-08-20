// +build release

package log

import (
	"github.com/sirupsen/logrus"
)

var RusPlus *Logrusplus

var StdLogger = logrus.StandardLogger()

var DefaultLogger = logrus.StandardLogger()
var SchedLogger = logrus.StandardLogger()

const (
	MaxFileSize     = 1024 * 1024 * 20
	DefaultMaxFiles = 2
	CoreMaxFiles    = 2
	Level           = logrus.InfoLevel
)
