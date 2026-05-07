package logger

import "go.uber.org/zap"

func NewLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		l, _ := zap.NewProduction()
		return l
	}
	return logger
}