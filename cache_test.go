// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package container

import "testing"

func TestLRUCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewLRUCache[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Get("a") // "b" is now the least recently used
	c.Put("c", 3)

	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Errorf("a = %v, %v; want 1, true", v, ok)
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len = %d; want 2", got)
	}
}

func TestLRUCacheOnEvict(t *testing.T) {
	type entry struct {
		key string
		val int
	}
	var got []entry
	c := NewLRUCacheWithOnEvict(2, func(k string, v int) {
		got = append(got, entry{k, v})
	})

	c.Put("a", 1)
	c.Put("b", 2)
	if len(got) != 0 {
		t.Fatalf("evicted %v before capacity was exceeded", got)
	}

	c.Put("c", 3)
	if len(got) != 1 {
		t.Fatalf("got %d evictions; want 1", len(got))
	}
	if got[0] != (entry{"a", 1}) {
		t.Errorf("evicted %v; want {a 1}", got[0])
	}
}

func TestLRUCacheNilOnEvict(t *testing.T) {
	c := NewLRUCache[string, int](1)
	c.Put("a", 1)
	c.Put("b", 2) // must not panic evicting "a" with no callback set
}
