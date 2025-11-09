package urlcount

import (
	"mapreduce/mr"
	"strconv"
	"strings"
)

// Map Extract URLs from web server logs
// Supports Apache/Nginx Common/Combined Log Format
// Example: 127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
func Map(filename string, contents string) []mr.KeyValue {
	lines := strings.Split(contents, "\n")
	var kva []mr.KeyValue

	for _, line := range lines {
		// Skip empty lines
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		// Extract URL from log line
		url := extractURL(line)
		if url != "" {
			kv := mr.KeyValue{Key: url, Value: "1"}
			kva = append(kva, kv)
		}
	}

	return kva
}

// extractURL extracts URL from a web server log line
// Supports formats like: "GET /path/to/resource HTTP/1.1"
func extractURL(line string) string {
	// Find the HTTP request part (between quotes)
	start := strings.Index(line, "\"")
	if start == -1 {
		return ""
	}

	end := strings.Index(line[start+1:], "\"")
	if end == -1 {
		return ""
	}

	// Extract request: "GET /path HTTP/1.1"
	request := line[start+1 : start+1+end]

	// Split by space to get: [GET, /path, HTTP/1.1]
	parts := strings.Fields(request)
	if len(parts) < 2 {
		return ""
	}

	// Return the URL (second part)
	return parts[1]
}

// Reduce Count URL access frequency
func Reduce(key string, values []string) string {
	count := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		count += n
	}
	return strconv.Itoa(count)
}
