package util

import (
	"strings"
	"fmt"
)

const (
	hostsPrefix       = "http://"
	secureHostsPrefix = "https://"
)

func PrepandProtocolIfAbsent(url string) string {
	if strings.HasPrefix(url, hostsPrefix) || strings.HasPrefix(url, secureHostsPrefix) {
		return url
	}
	return fmt.Sprintf("%s%s", hostsPrefix, url)
}
