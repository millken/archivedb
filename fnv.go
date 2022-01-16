package archivedb

import "math/bits"

const (
	// fnvOffset64 FNVa offset basis. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	fnvOffset64 uint64 = 0xcbf29ce484222325
	fnvOffset32 uint32 = 0x811c9dc5
	// fnvPrime64 FNVa prime value. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
	fnvPrime64 uint64 = 0x100000001b3
	fnvPrime32 uint32 = 0x01000193
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

// fnv32a gets the string and returns its uint32 hash value.
func fnv32a(key string) uint32 {
	var hash uint32 = fnvOffset32
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= fnvPrime32
	}

	return hash
}

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

// Sum32WithSeed is a port of MurmurHash3_x86_32 function.
func Sum32WithSeed(data []byte, seed uint32) uint32 {
	h1 := seed
	dlen := len(data)

	for len(data) >= 4 {
		k1 := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		data = data[4:]

		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	var k1 uint32
	switch len(data) {
	case 3:
		k1 ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(data[0])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(dlen)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}
