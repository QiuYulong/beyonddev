package skiplist

import (
	"fmt"
	"math/rand"
)

// IValue is data type interface which skiplist can handle.
// data in SkipList must implement this interface.
// NOTE: data is recommended to be immutable, or caller must promise not to change its value.
// otherwise it may break its orders in skiplist. data's order is computed by key when inserting.
type IValue interface {
	// CompareTo method return 0 if equal, negative int if less than, positive int if greater than.
	CompareTo(other interface{}) int
}

/* Thoughts behind the design.
[skiplist]
1.skiplist node: the node have two basic design, one is that make the data node and index node separated (used in
java's jdk ConcurrentSkipListMap implementation), the other is to unify them. I picked the second design. because I
think having one node type is better than having two, even though it makes implementation a bit harder. in this
way, the indexing node information is merged into the data node, so each data node will have multiple next pointers.
the nexts field are pointer slice. note that each node's size will not be changed in the life time, so this is
the most memory efficient/compact way.
2.backward iteration in skiplist: to provide the full support of backward ability, each node also need to track
all the previous nodes just like the next nodes. one trade off design is providing only one previous pointer to
the previous leaf node. it simplifies the design and saves memory with the cost that only leaf/data node backward
iteration is supported, index node backward iteration is not supported. but considering the backward iteration on
index nodes are rare, I choose to use the trade off design here.
3.Probability choice.
search cost: Ccpu = O((1/p)*log((1/p),N)), where N is length, 0 <= p < 1.
memory cost: Cmem = O(N/(1-p)), where N is length, 0 <= p < 1
search cost formula is a convex function, the global optimal point is at p=0.368.
memory cost formula is a monotonical increasing function, the global optimal point is at p=0.
considering the trade off of Ccpu and Cmem, 0.368 is the best pick, because Ccpu is critical while Cmem will not
be a big issue with current skiplist node's design. when p=0.368, Cmem=1.58N, in which, 1.0N is the data node, the
0.58N is just next pointers. so Cmem is acceptable when use optimal value of Ccpu.
[concurrency]
skiplist is lock-free, non-thread safe implementation. the lock will be implemented in caller to support more
complex requirements, like transaction.
--Yulong Qiu
*/

const (
	// Probability to lift up a node to higher level.
	Probability = 0.368
)

// node is the node of skiplist. internal use only.
type node struct {
	prev  *node
	nexts []*node
	value IValue
}

// level returns the level of node.
func (n *node) level() int {
	return len(n.nexts)
}

// SkipList is the type definition of SkipList.
// To simplify implementation, the internal skiplist is designed as a ring,
// the root node is a sentinel node which is not counted into len, and the root node is
// both the head and tail of skiplist. root's value is '-inf' as head, and 'inf' as tail in the ring.
type SkipList struct {
	root *node  //sentinel node, only root, root.prev, root.nexts are used.
	len  uint64 //current list length excluding sentinel node.
}

// NewSkipList create one ready to use SkipList.
func NewSkipList() *SkipList {
	var s SkipList
	s.root = new(node)
	s.root.nexts = append(s.root.nexts, s.root)
	s.root.prev = s.root
	s.len = 0
	return &s
}

// Len return the number of nodes in skiplist.
func (s *SkipList) Len() uint64 {
	return s.len
}

// Put inserts slv into s. return old value if exists, else nil.
// if slv not exists in s, then insert and return nil.
// if slv exists and replaceIfExists true, then replace old value by new value, then return old value.
// if slv exists but replaceIfExists false, do nothing and return old value.
func (s *SkipList) Put(slv IValue, replaceIfExists bool) (oldslv IValue) {
	nodes := s.floorNode(slv, true)
	if nodes[0] == s.root || nodes[0].value.CompareTo(slv) < 0 {
		// not exists: add and return nil.
		newNodeLvl := s.randomLevel()
		if newNodeLvl > s.level() { // if newNodeLvl is higher than s.level(), a new top level need to be added.
			s.root.nexts = append(s.root.nexts, s.root)
			nodes = append(nodes, s.root)
		}

		// init new node.
		newNode := new(node)
		newNode.nexts = make([]*node, newNodeLvl)
		newNode.value = slv

		// set previous pointer on leaf level.
		// NOTE: prev pointer only need to be set on leaf node. see explaination in beginning comments.
		newNode.prev = nodes[0]
		nodes[0].nexts[0].prev = newNode

		// set nexts pointers on each level.
		for lvl := 0; lvl < newNodeLvl; lvl++ {
			newNode.nexts[lvl] = nodes[lvl].nexts[lvl]
			nodes[lvl].nexts[lvl] = newNode
		}

		s.len++
		return nil
	}
	// exists: replace only if replaceIfExists true. return old value.
	old := nodes[0].value
	if replaceIfExists {
		nodes[0].value = slv
	}
	return old
}

// Remove removes value if value exists in skiplist, then return old value in skiplist.
// if value not exists, do nothing and return nil.
func (s *SkipList) Remove(slv IValue) IValue {
	nodes := s.floorNode(slv, true)
	if nodes[0] != s.root && nodes[0].value.CompareTo(slv) == 0 {
		// exists, need remove.
		value := nodes[0].value
		nodeLvl := nodes[0].level()
		nodes[0] = nodes[0].prev
		for i := 0; i < nodeLvl; i++ {
			nextnext := nodes[i].nexts[i].nexts[i]
			if i == 0 {
				nextnext.prev = nodes[0]
			}
			nodes[i].nexts[i] = nextnext
		}

		// delete top empty levels.
		lvl := s.level() - 1
		for s.root.nexts[lvl] == s.root && lvl > 0 {
			lvl--
		}
		s.root.nexts = s.root.nexts[:lvl+1]

		s.len--
		return value
	}
	return nil
}

// floorNode get the greatest leaf node which is less-equal-than slv.
// if withPath true, it also returns nodes in non-leaf levels that are less-than slv,
// else, only the leaf node returned.
func (s *SkipList) floorNode(slv IValue, withPath bool) []*node {
	nodes := make([]*node, 1)
	lvl := s.level()
	if withPath {
		nodes = make([]*node, lvl)
	}
	n := s.root
	// get non-leaf less-than nodes.
	for lvl--; lvl >= 0; lvl-- {
		// root's value is -inf as head, and inf as tail in the ring.
		if n == s.root || n.value.CompareTo(slv) < 0 {
			// traverse on level lvl forward.
			// when n.nexts[lvl] == s.root, it means tail(s.root) reached.
			for n.nexts[lvl] != s.root && n.nexts[lvl].value.CompareTo(slv) < 0 {
				n = n.nexts[lvl]
			}
		}
		if withPath {
			// the values of nodes on path are less than slv. (not less equal than).
			// which means, nodes on path are not floorNode, but strictly less-than nodes.
			nodes[lvl] = n
		}
	}
	// get leaf floor(less-equal-than) node.
	nodes[0] = n
	if n.nexts[0] != s.root && n.nexts[0].value.CompareTo(slv) == 0 {
		// if n.nexts[0] is the floorNode, move one step forward.
		nodes[0] = n.nexts[0]
	}
	return nodes
}

// level returns the total level of skiplist.
func (s *SkipList) level() int {
	return len(s.root.nexts)
}

// levelup returns true if node can be lifted to next level, else false.
func (s *SkipList) levelup() bool {
	return rand.Float64() < Probability
}

// randomLevel returns the level computed from Probability.
// for one inserting node, its initial level is 1.
// then with a probability p, its level may be 2, with probability p^2, its level may be 3.
// and so on, util the top level + 1.
func (s *SkipList) randomLevel() int {
	level := 1
	for s.levelup() && level < s.level()+1 {
		level++
	}
	return level
}

// Print prints the skiplist.
func (s *SkipList) Print() {
	for i := s.level() - 1; i >= 0; i-- {
		fmt.Printf("L%v: ", i)
		c := 0 //debug
		n := s.root.nexts[i]
		for n != s.root {
			c++ //debug
			fmt.Printf("%v  ", n.value)
			n = n.nexts[i]
		}
		fmt.Printf(", total %v", c)
		fmt.Print("\n")
	}
	fmt.Println()
}

// PrintValues prints the values in skiplist.
// @param forward, print from head to tail if true, print tail to head if false.
// @param offset, skip the first offset number of values when print. 0 or negative value will be ignored.
// @param limit, only print limit number of values. negative value will be ignored.
func (s *SkipList) PrintValues(forward bool, offset uint64, limit uint64) {
	it := s.Iterator(nil, forward, offset, limit)
	for val := it(); val != nil; val = it() {
		fmt.Printf("%v  ", val)
	}
	fmt.Println()
}

// Iterator returns one iterator with forward or backward order.
// posSlv: the beginning position's value where iterator start. the first iterated node is the one whose value is
//         cloest to posSlv in the travesal side. if posSlv is nil, then start position is root node.
//         (traversal side: next/bigger side if forward is true, smaller/prev side if forward is false)
// forward: if true, iterator traverse in direction from head to tail; else from tail to head.
// offset: skip the number of offset values.
// limit: output the number of limit values.
// NOTE: iterator time cost is unpredictable, without using lock. this causes data inconsistent sometime.
// because changes on iterated data is not visible to later iteration.
func (s *SkipList) Iterator(posSlv IValue, forward bool, offset uint64, limit uint64) func() IValue {
	var n *node // current node
	if posSlv == nil {
		n = s.root
	} else {
		n = s.floorNode(posSlv, false)[0]
		if forward {
			// make sure n is the previous node of first node.
			if n != s.root && n.value.CompareTo(posSlv) == 0 {
				n = n.prev
			}
			for offset > 0 {
				n = n.nexts[0]
				offset--
			}
		} else {
			// make sure n is the next node of first node.
			n = n.nexts[0]
			for offset > 0 {
				n = n.prev
				offset--
			}
		}
	}
	done := false
	return func() IValue {
		if done {
			return nil
		}
		if forward {
			n = n.nexts[0]
		} else {
			n = n.prev
		}
		if n == s.root || limit <= 0 {
			done = true
			return nil
		}
		limit--
		return n.value
	}
}
