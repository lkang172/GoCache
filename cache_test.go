package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPutAndGet(t *testing.T) {
	c := NewLRUCache(10)

	c.Put("key1", []byte("value1"), 0)

	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected value1, got %s", string(val))
	}
}

func TestGetMiss(t *testing.T) {
	c := NewLRUCache(10)

	_, ok := c.Get("nonexistent")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestUpdate(t *testing.T) {
	c := NewLRUCache(10)

	c.Put("key1", []byte("v1"), 0)
	c.Put("key1", []byte("v2"), 0)

	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %s", string(val))
	}

	if c.Len() != 1 {
		t.Fatalf("expected len 1, got %d", c.Len())
	}
}

func TestDelete(t *testing.T) {
	c := NewLRUCache(10)

	c.Put("key1", []byte("value1"), 0)

	if !c.Delete("key1") {
		t.Fatal("expected delete to return true")
	}

	_, ok := c.Get("key1")
	if ok {
		t.Fatal("expected key1 to be deleted")
	}

	if c.Delete("key1") {
		t.Fatal("expected delete of missing key to return false")
	}
}

func TestEviction(t *testing.T) {
	c := NewLRUCache(3)

	c.Put("a", []byte("1"), 0)
	c.Put("b", []byte("2"), 0)
	c.Put("c", []byte("3"), 0)
	c.Put("d", []byte("4"), 0) // should evict "a"

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected 'a' to be evicted")
	}

	if c.Len() != 3 {
		t.Fatalf("expected len 3, got %d", c.Len())
	}

	// Access "b" to make it recently used, then add "e" — "c" should be evicted
	c.Get("b")
	c.Put("e", []byte("5"), 0)

	if _, ok := c.Get("c"); ok {
		t.Fatal("expected 'c' to be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("expected 'b' to survive eviction")
	}
}

func TestTTLExpiry(t *testing.T) {
	c := NewLRUCache(10)

	c.Put("temp", []byte("data"), 50*time.Millisecond)

	val, ok := c.Get("temp")
	if !ok || string(val) != "data" {
		t.Fatal("expected temp to exist immediately after put")
	}

	time.Sleep(60 * time.Millisecond)

	_, ok = c.Get("temp")
	if ok {
		t.Fatal("expected temp to be expired")
	}
}

func TestTTLNoExpiry(t *testing.T) {
	c := NewLRUCache(10)

	c.Put("perm", []byte("data"), 0) // no TTL

	time.Sleep(50 * time.Millisecond)

	val, ok := c.Get("perm")
	if !ok || string(val) != "data" {
		t.Fatal("expected perm to still exist")
	}
}

func TestConcurrency(t *testing.T) {
	c := NewLRUCache(1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			c.Put(key, []byte("value"), 0)
			c.Get(key)
			c.Delete(key)
		}(i)
	}

	wg.Wait()
}

// Benchmarks

func BenchmarkPut(b *testing.B) {
	c := NewLRUCache(b.N)
	val := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i), val, 0)
	}
}

func BenchmarkGet(b *testing.B) {
	c := NewLRUCache(b.N)
	val := []byte("benchmark-value")

	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i), val, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkPutGet(b *testing.B) {
	c := NewLRUCache(b.N)
	val := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		c.Put(key, val, 0)
		c.Get(key)
	}
}

func BenchmarkConcurrentPutGet(b *testing.B) {
	c := NewLRUCache(b.N)
	val := []byte("benchmark-value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			c.Put(key, val, 0)
			c.Get(key)
			i++
		}
	})
}
