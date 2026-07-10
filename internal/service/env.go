package service

import "os"

// Env returns the value of environment variable key, or def if it is unset or
// empty. Services use this so the same binary runs both under docker-compose
// (where downstream hosts are service names) and locally (where they're
// localhost) without code changes.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
