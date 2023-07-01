package radixtree

import (
	"bytes"
)

// Tree is a radix tree of bytes keys and any values.
type Tree[T any] struct {
	root radixNode[T]
	size int
}

// New creates a new bytes-based radix tree
func New[T any]() *Tree[T] {
	return new(Tree[T])
}

type radixNode[T any] struct {
	// prefix is the edge label between this node and the parent, minus the key
	// segment used in the parent to index this child.
	prefix []byte
	edges  []edge[T]
	leaf   *leaf[T]
}

// WalkFunc is the type of the function called for each value visited by Walk
// or WalkPath. The key argument contains the elements of the key at which the
// value is stored.
//
// If the function returns true Walk stops immediately and returns. This
// applies to WalkPath as well.
type WalkFunc func(key []byte, value any) bool

// InspectFunc is the type of the function called for each node visited by
// Inspect. The key argument contains the key at which the node is located, the
// depth is the distance from the root of the tree, and children is the number
// of children the node has.
//
// If the function returns true Inspect stops immediately and returns.
type InspectFunc[T any] func(link, prefix, key []byte, depth, children int, hasValue bool, value T) bool

type leaf[T any] struct {
	key   []byte
	value T
}

type edge[T any] struct {
	radix byte
	node  *radixNode[T]
}

// Len returns the number of values stored in the tree.
func (t *Tree[T]) Len() int {
	return t.size
}

// Get returns the value stored at the given key. Returns false if there is no
// value present for the key.
func (t *Tree[T]) Get(key []byte) (value T, ok bool) {
	node := &t.root
	// Consume key data while mathcing edge and prefix; return if remaining key
	// data matches nothing.
	for len(key) != 0 {
		// Find edge for radix.
		node = node.getEdge(key[0])
		if node == nil {
			return value, false
		}

		// Consume key data.
		key = key[1:]
		if !bytes.HasPrefix(key, node.prefix) {
			return value, false
		}
		key = key[len(node.prefix):]
	}
	if node.leaf != nil {
		return node.leaf.value, true
	}
	return value, false
}

// Put inserts the value into the tree at the given key, replacing any existing
// items. It returns true if it adds a new value, false if it replaces an
// existing value.
func (t *Tree[T]) Put(key []byte, value T) bool {
	var (
		p          int
		isNewValue bool
		newEdge    edge[T]
		hasNewEdge bool
	)
	node := &t.root

	for i := 0; i < len(key); i++ {
		radix := key[i]
		if p < len(node.prefix) {
			if radix == node.prefix[p] {
				p++
				continue
			}
		} else if child := node.getEdge(radix); child != nil {
			node = child
			p = 0
			continue
		}
		// Descended as far as prefixes and edges match key, and still have key
		// data, so add child that has a prefix of the unmatched key data and
		// set its value to the new value.
		newChild := &radixNode[T]{
			leaf: &leaf[T]{
				key:   key,
				value: value,
			},
		}
		if i < len(key)-1 {
			newChild.prefix = key[i+1:]
		}
		newEdge = edge[T]{radix, newChild}
		hasNewEdge = true
		break
	}
	// Key has been consumed by traversing prefixes and/or edges, or has been
	// put into new child.

	// If key partially matches node's prefix, then need to split node.
	if p < len(node.prefix) {
		node.split(p)
		isNewValue = true
	}

	if hasNewEdge {
		node.addEdge(newEdge)
		isNewValue = true
		t.size++
	} else {
		// Store key at existing child
		if node.leaf == nil {
			isNewValue = true
			t.size++
		}
		node.leaf = &leaf[T]{
			key:   key,
			value: value,
		}
	}

	return isNewValue
}

// Delete removes the value associated with the given key. Returns true if
// there was a value stored for the key. If the node or any of its ancestors
// becomes childless as a result, they are removed from the tree.
func (t *Tree[T]) Delete(key []byte) bool {
	node := &t.root
	var (
		parents []*radixNode[T]
		links   []byte
	)
	for len(key) != 0 {
		parents = append(parents, node)

		// Find edge for radix.
		node = node.getEdge(key[0])
		if node == nil {
			// node does not exist.
			return false
		}
		links = append(links, key[0])

		// Consume key data.
		key = key[1:]
		if !bytes.HasPrefix(key, node.prefix) {
			return false
		}
		key = key[len(node.prefix):]
	}

	if node.leaf == nil {
		return false
	}

	// delete the node value, indicate that value was deleted.
	node.leaf = nil
	t.size--

	// If node is leaf, remove from parent. If parent becomes leaf, repeat.
	node = node.prune(parents, links)

	// If node has become compressible, compress it.
	if node != &t.root {
		node.compress()
	}

	return true
}

// DeletePrefix removes all values whose key is prefixed by the given prefix.
// Returns true if any values were removed.
func (t *Tree[T]) DeletePrefix(prefix []byte) bool {
	node := &t.root
	var (
		parents []*radixNode[T]
		links   []byte
	)
	for len(prefix) != 0 {
		parents = append(parents, node)

		// Find edge for radix.
		node = node.getEdge(prefix[0])
		if node == nil {
			// Node does not exist.
			return false
		}
		links = append(links, prefix[0])

		// Consume prefix.
		prefix = prefix[1:]
		if !bytes.HasPrefix(prefix, node.prefix) {
			if bytes.HasPrefix(node.prefix, prefix) {
				// Prefix consumed, so it prefixes every key from node down.
				break
			}
			return false
		}
		prefix = prefix[len(node.prefix):]
	}

	if node.edges != nil {
		var count int
		node.walk(func(k []byte, _ any) bool {
			count++
			return false
		})
		t.size -= count
		node.edges = nil
	} else {
		t.size--
	}
	node.leaf = nil

	// If node is leaf, remove from parent. If parent becomes leaf, repeat.
	node = node.prune(parents, links)

	// If node has become compressible, compress it.
	if node != &t.root {
		node.compress()
	}

	return true
}

// Walk visits all nodes whose keys match or are prefixed by the specified key,
// calling walkFn for each value found. If walkFn returns true, Walk returns.
// Use empty key "" to visit all nodes starting from the root or the Tree.
//
// The tree is traversed in lexical order, making the output deterministic.
func (t *Tree[T]) Walk(key []byte, walkFn WalkFunc) {
	node := &t.root
	for len(key) != 0 {
		if node = node.getEdge(key[0]); node == nil {
			return
		}

		// Consume key data
		key = key[1:]
		if !bytes.HasPrefix(key, node.prefix) {
			if bytes.HasPrefix(node.prefix, key) {
				break
			}
			return
		}
		key = key[len(node.prefix):]
	}

	// Walk down tree starting at node located at key.
	node.walk(walkFn)
}

// WalkPath walks each node along the path from the root to the node at the
// given key, calling walkFn for each node that has a value. If walkFn returns
// true, WalkPath returns.
//
// The tree is traversed in lexical order, making the output deterministic.
func (t *Tree[T]) WalkPath(key []byte, walkFn WalkFunc) {
	node := &t.root
	for {
		if node.leaf != nil && walkFn(node.leaf.key, node.leaf.value) {
			return
		}

		if len(key) == 0 {
			return
		}

		if node = node.getEdge(key[0]); node == nil {
			return
		}

		key = key[1:]
		if !bytes.HasPrefix(key, node.prefix) {
			return
		}
		key = key[len(node.prefix):]
	}
}

// Inspect walks every node of the tree, whether or not it holds a value,
// calling inspectFn with information about each node. This allows the
// structure of the tree to be examined and detailed statistics to be
// collected.
//
// If inspectFn returns false, the traversal is stopped and Inspect returns.
//
// The tree is traversed in lexical order, making the output deterministic.
func (t *Tree[T]) Inspect(inspectFn InspectFunc[T]) {
	t.root.inspect([]byte{}, []byte{}, 0, inspectFn)
}

// split splits a node such that a node:
//
//	("prefix", leaf, edges[])
//
// is split into parent branching node, and a child leaf node:
//
//	("pre", nil, edges[f])--->("ix", leaf, edges[])
func (node *radixNode[T]) split(p int) {
	split := &radixNode[T]{
		edges: node.edges,
		leaf:  node.leaf,
	}
	if p < len(node.prefix)-1 {
		split.prefix = node.prefix[p+1:]
	}
	node.edges = nil
	node.addEdge(edge[T]{node.prefix[p], split})
	if p == 0 {
		node.prefix = []byte{}
	} else {
		node.prefix = node.prefix[:p]
	}
	node.leaf = nil
}

func (node *radixNode[T]) prune(parents []*radixNode[T], links []byte) *radixNode[T] {
	if node.edges != nil {
		return node
	}
	// iterate parents towards root of tree, removing the empty leaf.
	for i := len(links) - 1; i >= 0; i-- {
		node = parents[i]
		node.delEdge(links[i])
		if len(node.edges) != 0 {
			// parent has other edges, stop.
			break
		}
		node.edges = nil
		if node.leaf != nil {
			// parent has a value, stop.
			break
		}
	}
	return node
}

func (node *radixNode[T]) compress() {
	if len(node.edges) != 1 || node.leaf != nil {
		return
	}
	edge := node.edges[0]
	var b bytes.Buffer
	b.Grow(len(node.prefix) + 1 + len(edge.node.prefix))
	b.Write(node.prefix)
	b.WriteByte(edge.radix)
	b.Write(edge.node.prefix)
	node.prefix = b.Bytes()
	node.leaf = edge.node.leaf
	node.edges = edge.node.edges
}

func (node *radixNode[T]) walk(walkFn WalkFunc) bool {
	if node.leaf != nil && walkFn(node.leaf.key, node.leaf.value) {
		return true
	}
	for _, edge := range node.edges {
		if edge.node.walk(walkFn) {
			return true
		}
	}
	return false
}

func (node *radixNode[T]) inspect(link, key []byte, depth int, inspectFn InspectFunc[T]) bool {
	key = append(key, link...)
	key = append(key, node.prefix...)
	var val T
	var hasVal bool
	if node.leaf != nil {
		val = node.leaf.value
		hasVal = true
	}
	if inspectFn(link, node.prefix, key, depth, len(node.edges), hasVal, val) {
		return true
	}
	for _, edge := range node.edges {
		if edge.node.inspect([]byte{edge.radix}, key, depth+1, inspectFn) {
			return true
		}
	}
	return false
}

// indexEdge binary searches for the edge index.
//
// This is faster then going through sort.Interface for repeated searches.
func (node *radixNode[T]) indexEdge(radix byte) int {
	n := len(node.edges)
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if node.edges[h].radix < radix {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

// getEdge binary searches for edge.
func (node *radixNode[T]) getEdge(radix byte) *radixNode[T] {
	idx := node.indexEdge(radix)
	if idx < len(node.edges) && node.edges[idx].radix == radix {
		return node.edges[idx].node
	}
	return nil
}

// addEdge binary searches to find where to insert edge, and inserts at.
func (node *radixNode[T]) addEdge(e edge[T]) {
	idx := node.indexEdge(e.radix)
	node.edges = append(node.edges, edge[T]{})
	copy(node.edges[idx+1:], node.edges[idx:])
	node.edges[idx] = e
}

// delEdge binary searches for edge and removes it.
func (node *radixNode[T]) delEdge(radix byte) {
	idx := node.indexEdge(radix)
	if idx < len(node.edges) && node.edges[idx].radix == radix {
		copy(node.edges[idx:], node.edges[idx+1:])
		node.edges[len(node.edges)-1] = edge[T]{}
		node.edges = node.edges[:len(node.edges)-1]
	}
}
