package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

var placeholderRe = regexp.MustCompile(`\$\{([^}]+)\}`)

// expandEnv resolves ${VAR} / ${VAR:-default} placeholders in raw config
// bytes before YAML parsing. Env wins even over an inline default. Variables
// with neither an env value nor a default are collected and reported as a
// single error naming every missing variable.
func expandEnv(data []byte) ([]byte, error) {
	var missing []string

	out := placeholderRe.ReplaceAllFunc(data, func(match []byte) []byte {
		inner := string(match[2 : len(match)-1]) // strip "${" and "}"
		varName, def, hasDefault := strings.Cut(inner, ":-")

		if v := os.Getenv(varName); v != "" {
			return []byte(v)
		}
		if hasDefault {
			return []byte(def)
		}
		missing = append(missing, varName)
		return match
	})

	if len(missing) > 0 {
		return nil, fmt.Errorf("config: unset environment variables: %s", strings.Join(missing, ", "))
	}
	return out, nil
}
