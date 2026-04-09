package lsm

import (
	"bytes"
	"fmt"
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

type MemtableEntry struct {
	Key        []byte
	VLogId     int
	VLogOffset int
}

type Memtable interface {
	// Insert key + vlog id + vlog offset
	Insert(MemtableEntry) error
	Read([]byte) (MemtableEntry, error)
}

type node struct {
	key        []byte
	vlogId     int
	vlogOffset int
	height     int
	left       *node
	right      *node
}
type AVL struct {
	root *node
}

func NewAVL() Memtable {
	return &AVL{}
}

func (a *AVL) Insert(entry MemtableEntry) error {
	n, err := a.insert(a.root, entry.Key, entry.VLogId, entry.VLogOffset)
	if err != nil {
		return err
	}
	a.root = n
	return nil
}
func (a *AVL) insert(curr *node, key []byte, id, offset int) (*node, error) {
	if curr == nil {
		return &node{
			key:        key,
			height:     1,
			vlogId:     id,
			vlogOffset: offset,
		}, nil
	}
	switch bytes.Compare(curr.key, key) {
	case 0:
		return curr, fmt.Errorf("key %s already exists", string(key))
	case 1:
		newNode, err := a.insert(curr.left, key, id, offset)
		if err != nil {
			return nil, err
		}
		curr.left = newNode
	case -1:
		newNode, err := a.insert(curr.right, key, id, offset)
		if err != nil {
			return nil, err
		}
		curr.right = newNode
	}

	// update height
	curr.height = 1 + max(curr.left.getHeight(), curr.right.getHeight())
	bf := curr.balanceFactor()

	if bf < -1 {
		// left rotations
		// curr.right.key < key
		if bytes.Compare(curr.right.key, key) == -1 {
			return curr.leftRotation(), nil
		} else {
			curr.right = curr.right.rightRotation()
			return curr.leftRotation(), nil
		}
	} else if bf > 1 {
		// right rotations
		// curr.left.key > key
		if bytes.Compare(curr.left.key, key) == 1 {
			return curr.rightRotation(), nil
		} else {
			curr.left = curr.left.leftRotation()
			return curr.rightRotation(), nil
		}
	}
	return curr, nil
}
func (a *AVL) Read(key []byte) (MemtableEntry, error) {
	curr := a.root
	for curr != nil {
		switch bytes.Compare(curr.key, key) {
		case 0:
			return MemtableEntry{Key: key, VLogOffset: curr.vlogOffset, VLogId: curr.vlogId}, nil
		case 1:
			curr = curr.left
		case -1:
			curr = curr.right
		}
	}
	return MemtableEntry{}, fmt.Errorf("%w: %s\n", ErrKeyNotFound, string(key))
}

//	               n               l          l
//	             /  \            /  \
//	            l       ->     ll    n
//	          /  \                  /
//					 ll		r                r
func (n *node) rightRotation() *node {
	l := n.left
	r := l.right

	n.left = r
	l.right = n

	// update heights
	n.height = 1 + max(n.left.getHeight(), n.right.getHeight())
	l.height = 1 + max(l.left.getHeight(), l.right.getHeight())

	return l
}

//	               n               r
//	             /  \            /  \
//	                r   ->      n   rr
//	              /  \        /  \
//					     l 	 rr           l
func (n *node) leftRotation() *node {
	r := n.right
	l := r.left

	n.right = l
	r.left = n

	// update heights
	n.height = 1 + max(n.left.getHeight(), n.right.getHeight())
	r.height = 1 + max(r.left.getHeight(), r.right.getHeight())

	return r
}
func (n *node) getHeight() int {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *node) balanceFactor() int {
	return n.left.getHeight() - n.right.getHeight()
}

func (a *AVL) InOrder() {
	a.inorder(a.root)
	fmt.Println()
}
func (a *AVL) inorder(curr *node) {
	if curr == nil {
		return
	}
	a.inorder(curr.left)
	fmt.Printf("%s ", string(curr.key))
	a.inorder(curr.right)
}
func (a *AVL) PreOrder() {
	a.preorder(a.root)
	fmt.Println()
}
func (a *AVL) preorder(curr *node) {
	if curr == nil {
		return
	}
	fmt.Printf("%s ", string(curr.key))
	a.preorder(curr.left)
	a.preorder(curr.right)
}
