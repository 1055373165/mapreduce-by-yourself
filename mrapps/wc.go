package mrapps

import (
	"mapreduce/mr"
	"strconv"
	"strings"
)

// Map Split text into words
func Map(filename string, contents string) []mr.KeyValue {
	// Tokenization function (only letters)
	ff := func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	}

	// Split text into words
	words := strings.FieldsFunc(contents, ff)

	var kva []mr.KeyValue
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}

	return kva
}

// Reduce Count words
func Reduce(key string, values []string) string {
	count := 0
	for _, v := range values {
		n, _ := strconv.Atoi(v)
		count += n
	}
	return strconv.Itoa(count)
}
