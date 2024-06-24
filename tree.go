package art

import (
	"bytes"
	"slices"
)

type tree[V any] struct {
	// version field is updated by each tree modification
	version int

	root *artNode[V]
	size int
}

// make sure that tree implements all methods from the Tree interface
var _ Tree[struct{}] = &tree[struct{}]{}

func (t *tree[V]) Insert(key Key, value V) (V, bool) {
	var oldValue V
	update := func(v *V) {
		oldValue = *v
		*v = value
	}
	updated := t.recursiveInsert(&t.root, key, func() (_ V) { return }, update, 0)
	if !updated {
		t.version++
		t.size++
	}

	return oldValue, updated
}

func (t *tree[V]) Update(key Key, create func() V, update func(*V)) (created bool) {
	if create == nil {
		create = func() (_ V) { return }
	}
	if update == nil {
		update = func(*V) {}
	}
	return !t.recursiveInsert(&t.root, key, create, update, 0)
}

func (t *tree[V]) Delete(key Key) (_ V, _ bool) {
	value, deleted := t.recursiveDelete(&t.root, key, 0)
	if deleted {
		t.version++
		t.size--
		return value, true
	}

	return
}

func (t *tree[V]) Search(key Key) (_ V, _ bool) {
	current := t.root
	depth := uint32(0)
	for current != nil {
		if current.isLeaf() {
			leaf := current.leaf()
			if leaf.match(key) {
				return leaf.value, true
			}

			return
		}

		curNode := current.node()

		if curNode.prefixLen > 0 {
			prefixLen := current.match(key, depth)
			if prefixLen != min(curNode.prefixLen, MaxPrefixLen) {
				return
			}
			depth += curNode.prefixLen
		}

		next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
		if *next != nil {
			current = *next
		} else {
			current = nil
		}
		depth++
	}

	return
}

func (t *tree[V]) SearchNearest(key Key) (nearest Key, value V, found bool) {
	current := t.root
	depth := uint32(0)
	var prevLeaf *leaf[V]
	for current != nil {
		if current.isLeaf() {
			leaf := current.leaf()
			if leaf.prefixMatchInverse(key) {
				return slices.Clone(leaf.key), leaf.value, true
			}
			return
		}

		curNode := current.node()

		if curNode.prefixLen > 0 {
			prefixLen := current.match(key, depth)
			if prefixLen != min(curNode.prefixLen, MaxPrefixLen) {
				if prevLeaf != nil {
					return slices.Clone(prevLeaf.key), prevLeaf.value, true
				}
				return
			}
			depth += curNode.prefixLen
		}

		minimum := current.minimum()
		next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
		if *next != nil {
			prevLeaf, current = minimum, *next
		} else {
			if minimum.prefixMatchInverse(key) {
				return slices.Clone(minimum.key), minimum.value, true
			}
			current = nil
		}
		depth++
	}

	return
}

func (t *tree[V]) Resolve(key Key, resolver func(key Key, conflictIndex int) (Key, int, int)) (value V, found bool) {
	doResolve := func(conflictIndex int) (edited bool, lower int, backtrack bool) {
		sub, lowerBound, upperBound := resolver(key, conflictIndex)
		if upperBound-lowerBound > 0 && upperBound >= conflictIndex && lowerBound <= conflictIndex && lowerBound >= 0 {
			if bytes.Equal(key[lowerBound:upperBound], sub) {
				return false, 0, false
			}
			key = slices.Replace([]byte(key), lowerBound, upperBound, sub...)
			if lowerBound < conflictIndex {
				return true, lowerBound, true
			}
			return true, lowerBound, false
		}
		return false, 0, false
	}

	type stackEntry struct {
		node  *artNode[V]
		depth uint32
	}

	stack := make([]stackEntry, 0, 4)
	stack = append(stack, stackEntry{t.root, 0})

LOOP:
	for {
		top := stack[len(stack)-1]
		current := top.node
		if current == nil {
			break
		}
		depth := top.depth
		if current.isLeaf() {
			leaf := current.leaf()

		MATCH:
			for {
				if leaf.match(key) {
					return leaf.value, true
				}

				// find the conflict index starting at depth
				limit := min(len(leaf.key), len(key))
				conflictIndex := int(depth)
				for ; conflictIndex < limit; conflictIndex++ {
					if leaf.key[conflictIndex] != key[conflictIndex] {
						break
					}
				}
				if edited, lowerBound, backtrack := doResolve(conflictIndex); edited {
					if backtrack {
						// step back until we reach the depth where the key was modified
						for stack[len(stack)-1].depth > uint32(lowerBound) {
							stack = stack[:len(stack)-1]
						}
						continue LOOP
					}
					continue MATCH
				}

				// if we get here, bail out - we have keys structured like this:
				// "a.b.c.d"
				// "a.*.c.e"
				//  ┌─>b──┐  ┌──>d
				//  │     ▼──┘
				//  a     c
				//  │     ▲──┐
				//  └─>*──┘  └──>e
				// s.t. making decisions for resolving paths after 'c' requires knowing
				// how we resolved paths before 'c'. solving this for all possible trees
				// requires much more complex backtracking that would increase the time
				// complexity of this search to an unreasonable level; it would likely
				// just be faster to iterate over all leaves and check them individually

				return
			}
		}

		curNode := current.node()

		if curNode.prefixLen > 0 {
			prefixLen := current.match(key, depth)
			if prefixLen != min(curNode.prefixLen, MaxPrefixLen) {
				if edited, lowerBound, backtrack := doResolve(int(depth)); edited {
					if backtrack {
						for stack[len(stack)-1].depth > uint32(lowerBound) {
							stack = stack[:len(stack)-1]
						}
					}
					continue LOOP
				}
				return
			}

			depth += curNode.prefixLen
		}

	FIND:
		for {
			next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
			if *next == nil {
				if edited, lowerBound, backtrack := doResolve(int(depth)); edited {
					if backtrack {
						for stack[len(stack)-1].depth > uint32(lowerBound) {
							stack = stack[:len(stack)-1]
						}
						continue LOOP
					}
					continue FIND
				}
				current = nil
			}
			stack = append(stack, stackEntry{*next, depth + 1})
			break
		}
	}

	return
}

func (t *tree[V]) Minimum() (value V, found bool) {
	if t == nil || t.root == nil {
		return
	}

	leaf := t.root.minimum()

	return leaf.value, true
}

func (t *tree[V]) Maximum() (value V, found bool) {
	if t == nil || t.root == nil {
		return
	}

	leaf := t.root.maximum()

	return leaf.value, true
}

func (t *tree[V]) Size() int {
	if t == nil || t.root == nil {
		return 0
	}

	return t.size
}

func (t *tree[V]) recursiveInsert(curNode **artNode[V], key Key, value func() V, update func(*V), depth uint32) (_ bool) {
	current := *curNode
	if current == nil {
		v := value()
		update(&v)
		replaceRef(curNode, newLeaf(key, v))
		return
	}

	if current.isLeaf() {
		leaf := current.leaf()

		// update exists value
		if leaf.match(key) {
			update(&leaf.value)
			return true
		}
		// new value, split the leaf into new node4
		v := value()
		update(&v)
		newLeaf := newLeaf(key, v)
		leaf2 := newLeaf.leaf()
		leafsLCP := t.longestCommonPrefix(leaf, leaf2, depth)

		newNode := newNode4[V]()
		newNode.setPrefix(key[depth:], leafsLCP)
		depth += leafsLCP

		newNode.addChild(leaf.key.charAt(int(depth)), leaf.key.valid(int(depth)), current)
		newNode.addChild(leaf2.key.charAt(int(depth)), leaf2.key.valid(int(depth)), newLeaf)
		replaceRef(curNode, newNode)

		return
	}

	node := current.node()
	if node.prefixLen > 0 {
		prefixMismatchIdx := current.matchDeep(key, depth)
		if prefixMismatchIdx >= node.prefixLen {
			depth += node.prefixLen
			goto NEXT_NODE
		}

		newNode := newNode4[V]()
		node4 := newNode.node()
		node4.prefixLen = prefixMismatchIdx
		for i := 0; i < int(min(prefixMismatchIdx, MaxPrefixLen)); i++ {
			node4.prefix[i] = node.prefix[i]
		}

		if node.prefixLen <= MaxPrefixLen {
			node.prefixLen -= (prefixMismatchIdx + 1)
			newNode.addChild(node.prefix[prefixMismatchIdx], true, current)

			for i, limit := uint32(0), min(node.prefixLen, MaxPrefixLen); i < limit; i++ {
				node.prefix[i] = node.prefix[prefixMismatchIdx+i+1]
			}

		} else {
			node.prefixLen -= (prefixMismatchIdx + 1)
			leaf := current.minimum()
			newNode.addChild(leaf.key.charAt(int(depth+prefixMismatchIdx)), leaf.key.valid(int(depth+prefixMismatchIdx)), current)

			for i, limit := uint32(0), min(node.prefixLen, MaxPrefixLen); i < limit; i++ {
				node.prefix[i] = leaf.key[depth+prefixMismatchIdx+i+1]
			}
		}

		// Insert the new leaf
		v := value()
		update(&v)
		newNode.addChild(key.charAt(int(depth+prefixMismatchIdx)), key.valid(int(depth+prefixMismatchIdx)), newLeaf(key, v))
		replaceRef(curNode, newNode)

		return
	}

NEXT_NODE:

	// Find a child to recursive to
	next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
	if *next != nil {
		return t.recursiveInsert(next, key, value, update, depth+1)
	}

	// No Child, artNode goes with us
	v := value()
	update(&v)
	current.addChild(key.charAt(int(depth)), key.valid(int(depth)), newLeaf(key, v))

	return
}

func (t *tree[V]) recursiveDelete(curNode **artNode[V], key Key, depth uint32) (_ V, _ bool) {
	if t == nil || *curNode == nil || len(key) == 0 {
		return
	}

	current := *curNode
	if current.isLeaf() {
		leaf := current.leaf()
		if leaf.match(key) {
			replaceRef(curNode, nil)
			return leaf.value, true
		}

		return
	}

	node := current.node()
	if node.prefixLen > 0 {
		prefixLen := current.match(key, depth)
		if prefixLen != min(node.prefixLen, MaxPrefixLen) {
			return
		}

		depth += node.prefixLen
	}

	next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
	if *next == nil {
		return
	}

	if (*next).isLeaf() {
		leaf := (*next).leaf()
		if leaf.match(key) {
			current.deleteChild(key.charAt(int(depth)), key.valid(int(depth)))
			return leaf.value, true
		}

		return
	}

	return t.recursiveDelete(next, key, depth+1)
}

func (t *tree[V]) longestCommonPrefix(l1 *leaf[V], l2 *leaf[V], depth uint32) uint32 {
	l1key, l2key := l1.key, l2.key
	idx, limit := depth, min(uint32(len(l1key)), uint32(len(l2key)))
	for ; idx < limit; idx++ {
		if l1key[idx] != l2key[idx] {
			break
		}
	}

	return idx - depth
}
