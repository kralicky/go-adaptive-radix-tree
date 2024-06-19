package art

import (
	"errors"
	"slices"
)

// A constant exposing all node types.
const (
	Leaf    Kind = 0
	Node4   Kind = 1
	Node16  Kind = 2
	Node48  Kind = 3
	Node256 Kind = 4
)

// Traverse Options.
const (
	// Iterate only over leaf nodes.
	TraverseLeaf = 1

	// Iterate only over non-leaf nodes.
	TraverseNode = 2

	// Iterate over all nodes in the tree.
	TraverseAll = TraverseLeaf | TraverseNode
)

// These errors can be returned when iteration over the tree.
var (
	ErrConcurrentModification = errors.New("Concurrent modification has been detected")
	ErrNoMoreNodes            = errors.New("There are no more nodes in the tree")
)

// Kind is a node type.
type Kind int

// Key Type.
// Key can be a set of any characters include unicode chars with null bytes.
type Key []byte

// Callback function type for tree traversal.
// if the callback function returns false then iteration is terminated.
type Callback[V any] func(node Node[V]) (cont bool)

// Node interface.
type Node[V any] interface {
	// Kind returns node type.
	Kind() Kind

	// Key returns leaf's key.
	// This method is only valid for leaf node,
	// if its called on non-leaf node then returns nil.
	Key() Key

	// Value returns leaf's value.
	// This method is only valid for leaf node,
	// if its called on non-leaf node then returns the zero value for V.
	Value() V
}

// Iterator iterates over nodes in key order.
type Iterator[V any] interface {
	// Returns true if the iteration has more nodes when traversing the tree.
	HasNext() bool

	// Returns the next element in the tree and advances the iterator position.
	// Returns ErrNoMoreNodes error if there are no more nodes in the tree.
	// Check if there is a next node with HasNext method.
	// Returns ErrConcurrentModification error if the tree has been structurally
	// modified after the iterator was created.
	Next() (Node[V], error)
}

// Tree is an Adaptive Radix Tree interface.
type Tree[V any] interface {
	// Insert a new key into the tree.
	// If the key already in the tree then return oldValue, true and nil, false otherwise.
	Insert(key Key, value V) (oldValue V, updated bool)

	// Update calls the provided update function to edit the value in-place. If
	// the key does not exist yet, it calls the provided create function first,
	// then the update function with the newly created value.
	Update(key Key, create func() V, update func(*V)) (created bool)

	// Delete removes a key from the tree and key's value, true is returned.
	// If the key does not exists then nothing is done and nil, false is returned.
	Delete(key Key) (value V, deleted bool)

	// Search returns the value of the specific key.
	// If the key exists then return value, true and nil, false otherwise.
	Search(key Key) (value V, found bool)

	// SearchNearest returns the value of the specific key, or if not found, the
	// value of the closest key less than the given key (price is right rules).
	SearchNearest(key Key) (nearest Key, value V, found bool)

	// Resolve attempts to search for the value of the specific key, calling the
	// provided function to try resolving conflicts by mutating parts of the key
	// during the tree traversal process.
	// Whenever a conflict is encountered, the resolver function will be called
	// with the current key and the index at which it encountered a conflict.
	// If the function returns a positive upperBound > conflictIndex, the bytes
	// [conflictIndex:upperBound) in the key will be replaced with substitution,
	// then the search will continue.
	Resolve(key Key, resolver func(key Key, conflictIndex int) (substitution Key, upperBound int)) (value V, found bool)

	// ForEach executes a provided callback once per leaf node by default.
	// The callback iteration is terminated if the callback function returns false.
	// Pass TraverseXXX as an options to execute a provided callback
	// once per NodeXXX type in the tree.
	ForEach(callback Callback[V], options ...int)

	// ForEachPrefix executes a provided callback once per leaf node that
	// leaf's key starts with the given keyPrefix.
	// The callback iteration is terminated if the callback function returns false.
	ForEachPrefix(keyPrefix Key, callback Callback[V])

	// Iterator returns an iterator for preorder traversal over leaf nodes by default.
	// Pass TraverseXXX as an options to return an iterator for preorder traversal over all NodeXXX types.
	Iterator(options ...int) Iterator[V]
	// IteratorPrefix(key Key) Iterator

	// Minimum returns the minimum valued leaf, true if leaf is found and nil, false otherwise.
	Minimum() (min V, found bool)

	// Maximum returns the maximum valued leaf, true if leaf is found and nil, false otherwise.
	Maximum() (max V, found bool)

	// Returns size of the tree
	Size() int
}

// New creates a new adaptive radix tree
func New[V any]() Tree[V] {
	return newTree[V]()
}

func (k Key) Compare(other Key) int {
	return slices.Compare(k, other)
}
