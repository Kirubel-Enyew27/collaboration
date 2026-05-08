package logger

import (
    "go.uber.org/zap"
)

// NewLogger returns a production zap logger.
func NewLogger() *zap.Logger {
    logger, err := zap.NewProduction()
    if err != nil {
        // Fallback to a no-op logger in the unlikely event of failure
        l, _ := zap.NewProduction(zap.Development())
        return l
    }
    return logger
}
