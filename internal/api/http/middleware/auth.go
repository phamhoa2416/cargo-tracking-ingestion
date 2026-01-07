package middleware

import (
	"crypto/rsa"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type JwtValidator struct {
	publicKey *rsa.PublicKey
	issuer    string
	audience  string
}

func NewJwtValidator(publicKeyPath, issuer, audience string) (*JwtValidator, error) {
	key, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key: %w", err)
	}

	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	return &JwtValidator{
		publicKey: publicKey,
		issuer:    issuer,
		audience:  audience,
	}, nil
}

func (j *JwtValidator) Validate() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			fmt.Printf("[AUTH] Missing Authorization header for path: %s\n", path)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid Authorization header"})
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			fmt.Printf("[AUTH] Invalid Authorization header format for path: %s (header: %s)\n", path, authHeader[:min(20, len(authHeader))])
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid Authorization header"})
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		fmt.Printf("[AUTH] Validating token for path: %s (token length: %d)\n", path, len(tokenString))

		claims := &jwt.RegisteredClaims{}
		token, err := jwt.ParseWithClaims(
			tokenString,
			claims,
			func(token *jwt.Token) (interface{}, error) {
				if token.Method.Alg() != jwt.SigningMethodRS512.Alg() {
					return nil, fmt.Errorf("unexpected signing method: %s, expected RS512", token.Method.Alg())
				}
				return j.publicKey, nil
			},
		)

		if err != nil {
			fmt.Printf("[AUTH] Token validation failed for path %s: %v\n", path, err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token", "details": err.Error()})
			return
		}

		if !token.Valid {
			fmt.Printf("[AUTH] Token is invalid for path: %s\n", path)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		fmt.Printf("[AUTH] Token validated successfully for path: %s, subject: %s\n", path, claims.Subject)
		c.Set("claims", claims)
		c.Set("user_id", claims.Subject)
		c.Next()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
