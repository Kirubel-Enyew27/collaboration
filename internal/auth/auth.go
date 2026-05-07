package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Auth struct {
	keys   map[string]string
	logger *zap.Logger
}

func New(keys map[string]string, logger *zap.Logger) *Auth {
	return &Auth{keys: keys, logger: logger}
}

func (a *Auth) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var key string
		if h := c.GetHeader("Authorization"); h != "" {
			if strings.HasPrefix(h, "ApiKey") {
				key = strings.TrimPrefix(h, "ApiKey")
			}
		}
		if key == "" {
			key = c.GetHeader("X-API-Key")
		}
		if key == "" {
			key = c.Query("api_key")
		}
		if key == "" {
			a.logger.Debug("missing api key")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing api key"})
			return 
		}
		user, ok := a.keys[key]
		if !ok {
			a.logger.Debug("invalid api key")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error":"invalid api key"})
			return 
		}

		c.Set("auth_user", user)
		c.Set("auth_api_key", key)
		c.Next()
	}
}