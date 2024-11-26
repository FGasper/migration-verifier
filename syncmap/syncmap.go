package syncmap

// This package wraps sync.Map to provide type safety via generics.

// Surprisingly, as of May 2023 there doesn’t appear to be a “go-to”
// public implementation of this. Initially we considered gopkg.in/typ.v4,
// but this largely reimplements sync.Map, which seems unideal for
// long-term maintenance and seems to lack wide adoption.
// github.com/puzpuzpuz/xsync is widely used but, because it attempts
// to iterate on sync.Map’s internals, requires map keys to be strings.
//
// Assumedly Go’s standard library will eventually add something like
// this, but for now this is a small, useful enough effort for us to
// have for ourselves.
//
// See this module’s tests for example usage.

import (
	"sync"
)

// SyncMap is a generics-powered replacement for sync.Map.
type SyncMap[KT comparable, VT any] struct {
	internalMap sync.Map
}

// Load behaves like sync.Map’s corresponding method, but
// if the value doesn’t exist then this returns the value type’s
// zero-value.
func (m *SyncMap[KT, VT]) Load(key KT) (VT, bool) {
	return m.returnLoad(m.internalMap.Load(key))
}

// Delete behaves like sync.Map’s corresponding method.
func (m *SyncMap[KT, VT]) Delete(key KT) {
	m.internalMap.Delete(key)
}

// Store behaves like sync.Map’s corresponding method.
func (m *SyncMap[KT, VT]) Store(key KT, value VT) {
	m.internalMap.Store(key, value)
}

// LoadAndDelete behaves like sync.Map’s corresponding method.
func (m *SyncMap[KT, VT]) LoadAndDelete(key KT) (VT, bool) {
	return m.returnLoad(m.internalMap.LoadAndDelete(key))
}

// LoadOrStore behaves like sync.Map’s corresponding method.
func (m *SyncMap[KT, VT]) LoadOrStore(key KT, value VT) (VT, bool) {
	actual, loaded := m.internalMap.LoadOrStore(key, value)

	return actual.(VT), loaded
}

// Range behaves like sync.Map’s corresponding method.
func (m *SyncMap[KT, VT]) Range(f func(KT, VT) bool) {
	m.internalMap.Range(func(key, value any) bool {
		return f(key.(KT), value.(VT))
	})
}

// ----------------------------------------------------------------------

// Len is a convenience method that returns the number of elements
// the map stores. It has no counterpart in sync.Map.
// This method is O(n); i.e., larger maps take longer.
func (m *SyncMap[KT, VT]) Len() int {
	mapLen := 0

	m.internalMap.Range(func(_, _ any) bool {
		mapLen++
		return true
	})

	return mapLen
}

// ----------------------------------------------------------------------

func (_ *SyncMap[KT, VT]) returnLoad(val any, loaded bool) (VT, bool) {
	if !loaded {
		return *new(VT), false
	}

	return val.(VT), true
}
