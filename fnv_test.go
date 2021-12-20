package archivedb

import (
	"hash/fnv"
	"testing"
)

type testCase struct {
	text         string
	expectedHash uint64
}

var testCases = []testCase{
	{"", stdLibFnvSum64("")},
	{"a", stdLibFnvSum64("a")},
	{"ab", stdLibFnvSum64("ab")},
	{"abc", stdLibFnvSum64("abc")},
	{"some longer and more complicated text", stdLibFnvSum64("some longer and more complicated text")},
}

func TestFnvHashSum64(t *testing.T) {
	for _, testCase := range testCases {
		hashed := fnv64a(testCase.text)
		if hashed != testCase.expectedHash {
			t.Errorf("hash(%q) = %d want %d", testCase.text, hashed, testCase.expectedHash)
		}
	}
}

func stdLibFnvSum64(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func BenchmarkFnvHashSum64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fnv64a(testCases[4].text)
	}
}

func BenchmarkFnvHashStdLibSum64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stdLibFnvSum64(testCases[4].text)
	}
}
