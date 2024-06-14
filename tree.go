package art

type tree[V any] struct {
	// version field is updated by each tree modification
	version int

	root *artNode[V]
	size int
}

// make sure that tree implements all methods from the Tree interface
var _ Tree[struct{}] = &tree[struct{}]{}

func (t *tree[V]) Insert(key Key, value V) (V, bool) {
	oldValue, updated := t.recursiveInsert(&t.root, key, value, 0)
	if !updated {
		t.version++
		t.size++
	}

	return oldValue, updated
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

func (t *tree[V]) recursiveInsert(curNode **artNode[V], key Key, value V, depth uint32) (_ V, _ bool) {
	current := *curNode
	if current == nil {
		replaceRef(curNode, newLeaf(key, value))
		return
	}

	if current.isLeaf() {
		leaf := current.leaf()

		// update exists value
		if leaf.match(key) {
			oldValue := leaf.value
			leaf.value = value
			return oldValue, true
		}
		// new value, split the leaf into new node4
		newLeaf := newLeaf(key, value)
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
		newNode.addChild(key.charAt(int(depth+prefixMismatchIdx)), key.valid(int(depth+prefixMismatchIdx)), newLeaf(key, value))
		replaceRef(curNode, newNode)

		return
	}

NEXT_NODE:

	// Find a child to recursive to
	next := current.findChild(key.charAt(int(depth)), key.valid(int(depth)))
	if *next != nil {
		return t.recursiveInsert(next, key, value, depth+1)
	}

	// No Child, artNode goes with us
	current.addChild(key.charAt(int(depth)), key.valid(int(depth)), newLeaf(key, value))

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
