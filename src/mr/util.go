package mr

import "log"

func LogPrint(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
func DLogPrint(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}