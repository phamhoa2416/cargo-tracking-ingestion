package middleware

import (
	"crypto/rsa"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"net/http"
	"os"
	"strings"
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
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing Authorization header"})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header format"})
			return
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return j.publicKey, nil
		})

		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			if iss, ok := claims["iss"].(string); !ok || iss != j.issuer {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid issuer"})
				return
			}

			if aud, ok := claims["aud"].(string); !ok || aud != j.audience {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid audience"})
				return
			}

			c.Set("claims", claims)
			c.Set("user_id", claims["sub"])
		} else {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token claims"})
			return
		}

		c.Next()
	}
}
