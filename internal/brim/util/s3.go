package util

import (
	"strings"
	"fmt"
)

//SplitKeyIntoBucketKey splits a string into a bucket and key
func SplitKeyIntoBucketKey(key string) (string, string, error) {
	keyParts := strings.SplitN(key, "/", 2)
	if len(keyParts) >= 2 {
		if keyParts[0] == "" {
			return "", "", fmt.Errorf("empty bucket part in key: %s", key)
		}
		return keyParts[0], keyParts[1], nil
	}
	return "", "", fmt.Errorf("wrong KEY (bucket/object) format - key: %s", key)
}
