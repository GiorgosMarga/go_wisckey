package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/GiorgosMarga/wisckey/internal/log"
)

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)

type MemtableEntry struct {
	Key        []byte
	VLogId     int64
	VLogOffset int64
}

func (me *MemtableEntry) Encode() []byte {
	buf := make([]byte, 24+len(me.Key))
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(me.Key)))
	offset += 8
	copy(buf[offset:], me.Key)
	offset += len(me.Key)
	binary.LittleEndian.PutUint64(buf[offset:], uint64(me.VLogId))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(me.VLogOffset))
	return buf
}

type Memtable interface {
	// Insert key + vlog id + vlog offset
	Insert(MemtableEntry) error
	Read([]byte) (*MemtableEntry, error)
	GetEntries() []*MemtableEntry
	Size() int
	DeleteLog() error
	Close() error
}

type node struct {
	key        []byte
	vlogId     int64
	vlogOffset int64
	height     int
	left       *node
	right      *node
}
type AVLMemtable struct {
	root     *node
	items    int
	currSize int
	mtx      *sync.RWMutex
	log      *log.Log
}

func NewAVLMemtable() *AVLMemtable {
	return &AVLMemtable{
		mtx: &sync.RWMutex{},
	}
}
func (a *AVLMemtable) Size() int {
	return a.currSize
}
func (a *AVLMemtable) Insert(entry MemtableEntry) error {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	// first write to log. Will skip for now since vLogs have both key and value so we can use it as WAL
	// if err := a.log.Write(entry.Encode()); err != nil {
	// 	return err
	// }

	n, err := a.insert(a.root, entry.Key, entry.VLogId, entry.VLogOffset)
	if err != nil {
		return err
	}
	a.root = n
	a.items++
	a.currSize += len(entry.Key)
	return nil
}
func (a *AVLMemtable) insert(curr *node, key []byte, id, offset int64) (*node, error) {
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
func (a *AVLMemtable) Read(key []byte) (*MemtableEntry, error) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()
	curr := a.root
	// check if entry is in the tree
	for curr != nil {
		switch bytes.Compare(curr.key, key) {
		case 0:
			return &MemtableEntry{Key: key, VLogOffset: curr.vlogOffset, VLogId: curr.vlogId}, nil
		case 1:
			curr = curr.left
		case -1:
			curr = curr.right
		}
	}

	return nil, fmt.Errorf("%w: %s\n", ErrKeyNotFound, string(key))
}

func (a *AVLMemtable) GetEntries() []*MemtableEntry {
	a.mtx.RLock()
	defer a.mtx.RUnlock()
	return a.getEntries(a.root, make([]*MemtableEntry, 0, a.items))
}
func (a *AVLMemtable) getEntries(curr *node, entries []*MemtableEntry) []*MemtableEntry {
	if curr == nil {
		return entries
	}
	entries = a.getEntries(curr.left, entries)
	entries = append(entries, &MemtableEntry{Key: curr.key, VLogId: curr.vlogId, VLogOffset: curr.vlogOffset})
	entries = a.getEntries(curr.right, entries)
	return entries
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

func (a *AVLMemtable) InOrder() {
	a.inorder(a.root)
	fmt.Println()
}
func (a *AVLMemtable) inorder(curr *node) {
	if curr == nil {
		return
	}
	a.inorder(curr.left)
	fmt.Printf("%s ", string(curr.key))
	a.inorder(curr.right)
}
func (a *AVLMemtable) PreOrder() {
	a.preorder(a.root)
	fmt.Println()
}
func (a *AVLMemtable) preorder(curr *node) {
	if curr == nil {
		return
	}
	fmt.Printf("%s ", string(curr.key))
	a.preorder(curr.left)
	a.preorder(curr.right)
}
func (a *AVLMemtable) DeleteLog() error {
	return a.log.Delete()
}
func (a *AVLMemtable) Close() error {
	return nil
}
