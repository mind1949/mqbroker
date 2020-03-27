package mqbroker

import "log"

var IsDebug bool

func debugf(format string, a ...interface{}) {
	if !IsDebug {
		return
	}
	log.Printf(format, a...)
}
