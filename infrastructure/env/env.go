package env

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func GetNameWithEnvPrefix(name string) string {
	return fmt.Sprintf("%s.%s", GetEnvName(), name)
}

// GetEnvOrDefault gives env variable name or default value
func GetEnvOrDefault(key, defaultValue string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return defaultValue
}

// GetEnvOrDefault gives env name
func GetEnvName() string {
	env := GetEnvOrDefault("ENV", "PROD")
	return strings.ToLower(env)
}

// FailOnError logs Fatal when erorr=
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
