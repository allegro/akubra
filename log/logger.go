package log

import (
	"io"
	"log/syslog"
	"os"

	"github.com/sirupsen/logrus"
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
}

// LoggerConfig holds oprions
type LoggerConfig struct {
	Stderr    bool   `yaml:"stderr,omitempty"`
	PlainText bool   `yaml:"plaintext,omitempty"`
	Stdout    bool   `yaml:"stdout,omitempty"`
	File      string `yaml:"file,omitempty"`
	Syslog    string `yaml:"syslog,omitempty"`
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
	return []byte(entry.Message + "\n"), nil
}

// NewLogger creates Logger
func NewLogger(config LoggerConfig) (Logger, error) {
	writer, err := createLogWriter(config)
	if err != nil {
		return nil, err
	}

	var formatter logrus.Formatter

	formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}

	if config.PlainText {
		formatter = PlainTextFormatter{}
	}

	logger := &logrus.Logger{
		Out:       writer,
		Formatter: formatter,
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	return logger, nil
}

// DefaultLogger ...
var DefaultLogger = logrus.New()

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
