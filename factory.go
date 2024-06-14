package art

import (
	"unsafe"
)

type nodeFactory[V any] interface {
	newNode4() *artNode[V]
	newNode16() *artNode[V]
	newNode48() *artNode[V]
	newNode256() *artNode[V]
	newLeaf(key Key, value V) *artNode[V]
}

func newTree[V any]() *tree[V] {
	return &tree[V]{}
}

// Simple obj factory implementation
func newNode4[V any]() *artNode[V] {
	return &artNode[V]{kind: Node4, ref: unsafe.Pointer(new(node4[V]))}
}

func newNode16[V any]() *artNode[V] {
	return &artNode[V]{kind: Node16, ref: unsafe.Pointer(&node16[V]{})}
}

func newNode48[V any]() *artNode[V] {
	return &artNode[V]{kind: Node48, ref: unsafe.Pointer(&node48[V]{})}
}

func newNode256[V any]() *artNode[V] {
	return &artNode[V]{kind: Node256, ref: unsafe.Pointer(&node256[V]{})}
}

func newLeaf[V any](key Key, value V) *artNode[V] {
	clonedKey := make(Key, len(key))
	copy(clonedKey, key)
	return &artNode[V]{
		kind: Leaf,
		ref:  unsafe.Pointer(&leaf[V]{key: clonedKey, value: value}),
	}
}
