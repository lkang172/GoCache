package main

import (
	"container/list"
	"sync"
	"time"
)

type Item struct {
	Key    string
	Value  []byte
	Expiry time.Time
}

func (i *Item) isExpired() bool {
	return !i.Expiry.IsZero() && time.Now().After(i.Expiry)
}

type LRUCache struct {
	capacity  int
	items     map[string]*list.Element
	evictList *list.List
	mu        sync.RWMutex
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity:  capacity,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

func (c *LRUCache) removeOldest() {
	tail := c.evictList.Back()

	if tail != nil {
		c.evictList.Remove(tail)

		item := tail.Value.(*Item)

		delete(c.items, item.Key)
	}
}

func (c *LRUCache) removeElement(el *list.Element) {
	c.evictList.Remove(el)
	item := el.Value.(*Item)
	delete(c.items, item.Key)
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		return nil, false
	}

	item := element.Value.(*Item)

	if item.isExpired() {
		c.removeElement(element)
		return nil, false
	}

	c.evictList.MoveToFront(element)
	return item.Value, true
}

func (c *LRUCache) Put(key string, value []byte, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}

	if element, ok := c.items[key]; ok {
		item := element.Value.(*Item)
		item.Value = value
		item.Expiry = expiry
		c.evictList.MoveToFront(element)
	} else {
		item := &Item{Key: key, Value: value, Expiry: expiry}
		node := c.evictList.PushFront(item)
		c.items[key] = node

		if c.evictList.Len() > c.capacity {
			c.removeOldest()
		}
	}
	return true
}

func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		return false
	}

	c.removeElement(element)
	return true
}

func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

func (c *LRUCache) cleanup() {
	for {
		time.Sleep(500 * time.Millisecond)

		c.mu.Lock()
		for _, el := range c.items {
			item := el.Value.(*Item)
			if item.isExpired() {
				c.removeElement(el)
			}
		}
		c.mu.Unlock()
	}
}
