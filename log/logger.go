package log

import (
	"io"
	"log/syslog"
	"os"
	"strings"
	"time"

	"github.com/allegro/akubra/log/sql"
	"github.com/sirupsen/logrus"
)

// ContextKey is type for log related context keys
type ContextKey string

const (
	// ContextreqIDKey is Request Context Value key for debug logging
	ContextreqIDKey = ContextKey("ContextreqIDKey")
)

// SyslogFacilityMap is string map of facilities
var SyslogFacilityMap = map[string]syslog.Priority{
	"LOG_KERN":     syslog.LOG_KERN,
	"LOG_USER":     syslog.LOG_USER,
	"LOG_MAIL":     syslog.LOG_MAIL,
	"LOG_DAEMON":   syslog.LOG_DAEMON,
	"LOG_AUTH":     syslog.LOG_AUTH,
	"LOG_SYSLOG":   syslog.LOG_SYSLOG,
	"LOG_LPR":      syslog.LOG_LPR,
	"LOG_NEWS":     syslog.LOG_NEWS,
	"LOG_UUCP":     syslog.LOG_UUCP,
	"LOG_CRON":     syslog.LOG_CRON,
	"LOG_AUTHPRIV": syslog.LOG_AUTHPRIV,
	"LOG_FTP":      syslog.LOG_FTP,
	"LOG_LOCAL0":   syslog.LOG_LOCAL0,
	"LOG_LOCAL1":   syslog.LOG_LOCAL1,
	"LOG_LOCAL2":   syslog.LOG_LOCAL2,
	"LOG_LOCAL3":   syslog.LOG_LOCAL3,
	"LOG_LOCAL4":   syslog.LOG_LOCAL4,
	"LOG_LOCAL5":   syslog.LOG_LOCAL5,
	"LOG_LOCAL6":   syslog.LOG_LOCAL6,
	"LOG_LOCAL7":   syslog.LOG_LOCAL7,
}

// LogLevelMap is string map of log levels
var LogLevelMap = map[string]logrus.Level{
	"Panic": logrus.PanicLevel,
	"Fatal": logrus.FatalLevel,
	"Error": logrus.ErrorLevel,
	"Warn":  logrus.WarnLevel,
	"Info":  logrus.InfoLevel,
	"Debug": logrus.DebugLevel,
}

// Logger interface generalizes Logger implementations
type Logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})

	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})

	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Debugln(v ...interface{})
}

// LoggerConfig holds oprions
type LoggerConfig struct {
	Stderr    bool         `yaml:"stderr,omitempty"`
	PlainText bool         `yaml:"plaintext,omitempty"`
	Stdout    bool         `yaml:"stdout,omitempty"`
	File      string       `yaml:"file"`
	Syslog    string       `yaml:"syslog"`
	Database  sql.DBConfig `yaml:"database"`
	Level     string       `yaml:"level"`
}

func createLogWriter(config LoggerConfig) (io.Writer, error) {
	var writers []io.Writer
	if facility, ok := SyslogFacilityMap[config.Syslog]; ok {
		writer, err := syslog.New(facility, "")
		if err != nil {
			return nil, err
		}
		writers = append(writers, writer)
	}
	if config.Stderr {
		writers = append(writers, os.Stderr)
	}
	if config.Stdout {
		writers = append(writers, os.Stdout)
	}
	if config.File != "" {
		f, err := os.OpenFile(config.File, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}
		writers = append(writers, f)
	}
	return io.MultiWriter(writers...), nil
}

// PlainTextFormatter implements raw message formatting
type PlainTextFormatter struct{}

// Format implements logrus.Formatter interface
func (f PlainTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	return []byte(strings.TrimSuffix(entry.Message, "\n") + "\n"), nil
}

type stripMessageNewLineFormatter struct {
	logrus.Formatter
}

// Format implements logrus.Formatter interface
func (f stripMessageNewLineFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Message = strings.TrimSuffix(entry.Message, "\n")
	return f.Formatter.Format(entry)
}

func createHooks(config LoggerConfig) (lh logrus.LevelHooks, err error) {
	emptyConf := sql.DBConfig{}
	lh = make(logrus.LevelHooks)
	if config.Database != emptyConf {
		hook, nserr := sql.NewSyncLogPsqlHook(config.Database)
		if nserr != nil {
			return lh, nserr
		}
		hooks, ok := lh[logrus.InfoLevel]
		if !ok {
			lh[logrus.InfoLevel] = []logrus.Hook{hook}
		} else {
			lh[logrus.InfoLevel] = append(hooks, hook)
		}
	}
	return
}

// NewLogger creates Logger
func NewLogger(config LoggerConfig) (Logger, error) {

	writer, err := createLogWriter(config)
	if err != nil {
		return nil, err
	}

	var formatter logrus.Formatter

	formatter = stripMessageNewLineFormatter{
		&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.StampMicro,
		}}

	if config.PlainText {
		formatter = PlainTextFormatter{}
	}

	level := logrus.DebugLevel

	if conflevel, ok := LogLevelMap[config.Level]; ok {
		level = conflevel
	}
	hooks, err := createHooks(config)
	if err != nil {
		return nil, err
	}
	logger := &logrus.Logger{
		Out:       writer,
		Formatter: formatter,
		Hooks:     hooks,
		Level:     level,
	}
	return logger, nil
}

// DefaultLogger ...
var DefaultLogger Logger = logrus.New()

// Fatal calls DefaultLogger
func Fatal(v ...interface{}) {
	DefaultLogger.Fatal(v...)
}

// Fatalf calls DefaultLogger
func Fatalf(format string, v ...interface{}) {
	DefaultLogger.Fatalf(format, v...)
}

// Fatalln calls DefaultLogger
func Fatalln(v ...interface{}) {
	DefaultLogger.Fatalln(v...)
}

// Panic calls DefaultLogger
func Panic(v ...interface{}) {
	DefaultLogger.Panic(v...)
}

// Panicf calls DefaultLogger
func Panicf(format string, v ...interface{}) {
	DefaultLogger.Panicf(format, v...)
}

// Panicln calls DefaultLogger
func Panicln(v ...interface{}) {
	DefaultLogger.Panicln(v...)
}

// Print calls DefaultLogger
func Print(v ...interface{}) {
	DefaultLogger.Print(v...)
}

// Printf calls DefaultLogger
func Printf(format string, v ...interface{}) {
	DefaultLogger.Printf(format, v...)
}

// Println calls DefaultLogger
func Println(v ...interface{}) {
	DefaultLogger.Println(v...)
}

// Debug calls DefaultLogger
func Debug(v ...interface{}) {
	DefaultLogger.Debug(v...)
}

// Debugf calls DefaultLogger
func Debugf(format string, v ...interface{}) {
	DefaultLogger.Debugf(format, v...)
}

// Debugln calls DefaultLogger
func Debugln(v ...interface{}) {
	DefaultLogger.Debugln(v...)
}
