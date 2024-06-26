package cassandra

type Logger interface {
	Debug(args ...interface{})
	Debugf(pattern string, args ...interface{})
	Log(args ...interface{})
	Logf(pattern string, args ...interface{})
	Error(args ...interface{})
	Errorf(patter string, args ...interface{})
	Info(args ...interface{})
	Infof(patter string, args ...interface{})
}
