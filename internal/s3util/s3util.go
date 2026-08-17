package s3util

import "strings"

// ShouldProcess reports whether key is eligible for processing: false if key
// ends with "/" (folder marker), false if key doesn't end with ".json",
// false if processedPrefix != "" && key already starts with it.
func ShouldProcess(key, processedPrefix string) bool {
	if strings.HasSuffix(key, "/") {
		return false
	}
	if !strings.HasSuffix(key, ".json") {
		return false
	}
	if processedPrefix != "" && strings.HasPrefix(key, processedPrefix) {
		return false
	}
	return true
}

// ProcessedKey returns the destination key when moving key under
// processedPrefix (original path preserved under the new prefix).
func ProcessedKey(processedPrefix, key string) string {
	return processedPrefix + key
}
