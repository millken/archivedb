package archivedb

const (
	// fnvOffset64 FNVa offset basis. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	fnvOffset64 uint64 = 0xcbf29ce484222325
	// fnvPrime64 FNVa prime value. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	fnvPrime64 uint64 = 0x100000001b3
)

// fnv64a gets the string and returns its uint64 hash value.
func fnv64a(key string) uint64 {
	var hash uint64 = fnvOffset64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= fnvPrime64
	}

	return hash
}
