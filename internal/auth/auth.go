package auth

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Auth struct {
    keys   map[string]string // apiKey -> userID
    logger *zap.Logger
}

// New creates an Auth with a provided map of apiKey->userID.
func New(keys map[string]string, logger *zap.Logger) *Auth {
    return &Auth{keys: keys, logger: logger}
}

// Middleware returns a Gin middleware that validates API keys provided via
// Authorization header ("ApiKey <key>") or X-API-Key header or ?api_key query param.
func (a *Auth) Middleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        var key string
        // 1. Authorization: ApiKey <key>
        if h := c.GetHeader("Authorization"); h != "" {
            if strings.HasPrefix(h, "ApiKey ") {
                key = strings.TrimPrefix(h, "ApiKey ")
            }
        }
        // 2. X-API-Key
        if key == "" {
            key = c.GetHeader("X-API-Key")
        }
        // 3. query param
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
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
            return
        }

        // store the user id or api key in context for handlers
        c.Set("auth_user", user)
        c.Set("auth_api_key", key)
        c.Next()
    }
}
