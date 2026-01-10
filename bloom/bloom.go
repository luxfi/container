// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package bloom

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

var (
	ErrInvalidNumHashes  = errors.New("numHashes must be positive")
	ErrInvalidNumEntries = errors.New("numEntries must be positive")
)

// OptimalParameters returns optimal numHashes and numEntries for a bloom filter
// given the expected number of elements and desired false positive probability.
func OptimalParameters(maxN int, p float64) (numHashes int, numEntries int) {
	if maxN <= 0 || p <= 0 || p >= 1 {
		return 1, 1
	}
	n := float64(maxN)
	m := -1 * n * math.Log(p) / (math.Ln2 * math.Ln2)
	k := m / n * math.Ln2
	numEntries = int(math.Ceil(m / 8)) // entries in bytes
	numHashes = int(math.Ceil(k))
	if numEntries < 1 {
		numEntries = 1
	}
	if numHashes < 1 {
		numHashes = 1
	}
	return numHashes, numEntries
}

// Hash computes a 64-bit hash of the given bytes.
// The extra parameter allows for additional entropy.
func Hash(b []byte, extra []byte) uint64 {
	// Simple FNV-1a hash
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)
	h := uint64(fnvOffset)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	for _, c := range extra {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// Filter is a thread-safe bloom filter.
type Filter struct {
	mu         sync.RWMutex
	bits       []byte
	numHashes  int
	numEntries int
}

// New creates a new bloom filter with the given parameters.
func New(numHashes, numEntries int) (*Filter, error) {
	if numHashes <= 0 {
		return nil, ErrInvalidNumHashes
	}
	if numEntries <= 0 {
		return nil, ErrInvalidNumEntries
	}
	return &Filter{
		bits:       make([]byte, numEntries),
		numHashes:  numHashes,
		numEntries: numEntries,
	}, nil
}

// Add adds the hash to the filter.
func (f *Filter) Add(hash uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	h1 := uint32(hash)
	h2 := uint32(hash >> 32)
	numBits := uint32(f.numEntries * 8)

	for i := 0; i < f.numHashes; i++ {
		bit := (h1 + uint32(i)*h2) % numBits
		byteIdx := bit / 8
		bitIdx := bit % 8
		f.bits[byteIdx] |= 1 << bitIdx
	}
}

// Contains checks if the hash might be in the filter.
func (f *Filter) Contains(hash uint64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	h1 := uint32(hash)
	h2 := uint32(hash >> 32)
	numBits := uint32(f.numEntries * 8)

	for i := 0; i < f.numHashes; i++ {
		bit := (h1 + uint32(i)*h2) % numBits
		byteIdx := bit / 8
		bitIdx := bit % 8
		if f.bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary returns the binary representation of the filter.
func (f *Filter) MarshalBinary() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Format: 1 byte numHashes + 8 bytes numEntries + bits
	buf := make([]byte, 1+8+len(f.bits))
	buf[0] = byte(f.numHashes)
	binary.BigEndian.PutUint64(buf[1:9], uint64(f.numEntries))
	copy(buf[9:], f.bits)
	return buf, nil
}

// UnmarshalBinary restores the filter from binary representation.
func (f *Filter) UnmarshalBinary(data []byte) error {
	if len(data) < 9 {
		return errors.New("data too short")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.numHashes = int(data[0])
	f.numEntries = int(binary.BigEndian.Uint64(data[1:9]))
	f.bits = make([]byte, len(data)-9)
	copy(f.bits, data[9:])
	return nil
}
