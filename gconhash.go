/*
 MIT License
 Copyright (c) 2019 Max Kuznetsov <syhpoon@syhpoon.ca>
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/

package gconhash

import (
	"math"
	"math/rand"
	"sort"

	"github.com/spaolacci/murmur3"
)

// The problem we're trying to solve is briefly the following:
// We have a cluster of N nodes and M entities.
// We need to distribute these M entities over N nodes more-or-less uniformly,
// that is each node should get roughly the same amount of entities.
// When a new node joins the cluster we want to "steal" roughly N/M entities
// from old nodes and allocate them to the new one.
// Conversely, when an existing node leaves the cluster, we want to re-distribute
// its entities uniformly among remaining nodes.
// Additional complication is that we want to do it in a completely
// stateless manner, that is every node should be able to compute the same
// allocation plan without talking to other nodes, based purely on the amount
// of nodes and their names (this information SHOULD be somehow disseminated among nodes).

// The way this algorithm works is the following:
// * Every entity id is hashed (using Murmur3) into a uint64 number.
// * The entire hash space (0 - math.MaxUint64) is split into Q equal regions.
// * A pseudo-random source is created for every node with a seed being the hashed
//   node name.
// * Then for every region in order we need to assign a node to it, and it's done
//   by iterating over node ids in sorted order and looking for the first node
//   whose random source produces the current range id.
//   This node is then assigned to the range.
// * After we found the node for the range, we need to synchronize the remaining
//   random sources - iterate over all of them (excluding selected one) until
//   the same range id is produced once for each.
//   This step is needed in order to avoid major perturbations in the allocation
//   plan during node departures/arrivals.
// * After having assigned a range to the node Q/N times, a node is removed from
//   the list of available for allocation node.
// * The process repeats until the allocation list is empty.
// * After allocation is done, in order to find which node is responsible for
//   a given entity, we simply hash it and use binary search on the allocation plan
//   to find the node which contains the region containing the key hash.

// Hasher struct is used to assign a list of ids to the hash space ring
// and to determine which id is responsible for a given key.
type Hasher struct {
	ids       []string
	seed      uint32
	ranges    int
	rangeSize uint64
	// range idx -> peer id
	rangeAllocations []string
	// Range idx -> upper id bound
	// For example, given rangeValues = [100, 500, 1000]
	// means that range 0 is [0, 100),
	//            range 1 is [100, 500)
	//            range 2 is [500, 1000)
	//            range 3 is [1000, math.MaxUint64)
	rangeValues []uint64
}

// ids must not be empty!
func New(ids []string, ranges int, seed uint32) *Hasher {
	sort.Strings(ids)

	rangeSize := math.MaxUint64 / uint64(ranges)
	rangeValues := make([]uint64, ranges)
	curRangeVal := uint64(0)

	idx := 0

	for idx < ranges {
		rangeValues[idx] = curRangeVal + rangeSize

		idx++
		curRangeVal += rangeSize
	}

	hasher := &Hasher{
		ids:              ids,
		seed:             seed,
		ranges:           ranges,
		rangeSize:        rangeSize,
		rangeAllocations: make([]string, ranges),
		rangeValues:      rangeValues,
	}

	share := ranges / len(ids)
	// We need to always allocate a full amount of ranges
	rem := ranges % len(ids)

	countReqs := map[string]int{}

	for _, id := range ids {
		additional := 0

		if rem > 0 {
			additional = 1
			rem--
		}

		countReqs[id] = share + additional
	}

	hp := newHashPool(ids, ranges, countReqs, hasher)

	plan := map[string][]int{}

	for r := 0; r < ranges; r++ {
		id := hp.allocate(r)

		hasher.rangeAllocations[r] = id
		plan[id] = append(plan[id], r)
	}

	return hasher
}

func (h *Hasher) Hash(key string) uint64 {
	return murmur3.Sum64WithSeed([]byte(key), h.seed)
}

// Given a key, find an id (a peer) responsible for it
func (h *Hasher) IdForKey(key string) string {
	return h.rangeAllocations[h.RangeForKey(key)]
}

// Get all the ranges for a given id (a peer)
func (h *Hasher) Ranges(id string) []int {
	var ranges []int

	for rid, peer := range h.rangeAllocations {
		if peer == id {
			ranges = append(ranges, rid)
		}
	}

	return ranges
}

// Return a range which a key falls into
func (h *Hasher) RangeForKey(key string) int {
	return h.search(h.Hash(key), 0, h.ranges)
}

func (h *Hasher) search(hash uint64, l, r int) int {
	if l > r {
		return 0
	}

	m := (l + r) / 2
	cur := h.rangeValues[m]

	if hash >= cur-h.rangeSize && hash < cur {
		return m
	} else if hash < cur {
		return h.search(hash, l, m-1)
	} else {
		return h.search(hash, m+1, r)
	}
}

type hashPool struct {
	ids       []string
	ranges    int
	hasher    *Hasher
	rsources  map[string]*rand.Rand
	idMatched map[string]int
	countReqs map[string]int
}

func newHashPool(ids []string, ranges int, countReqs map[string]int, hasher *Hasher) *hashPool {
	hp := &hashPool{
		ids:       ids,
		hasher:    hasher,
		rsources:  map[string]*rand.Rand{},
		idMatched: map[string]int{},
		countReqs: countReqs,
		ranges:    ranges,
	}

	for _, id := range ids {
		h := hasher.Hash(id)
		hp.rsources[id] = rand.New(rand.NewSource(int64(h)))

	}

	return hp
}

func (hp *hashPool) allocate(r int) string {
	foundId := ""
	deleteIdx := -1

MAIN:
	for {
		for idx, id := range hp.ids {
			val := hp.rsources[id].Intn(hp.ranges)

			if val == r {
				hp.sync(r, map[string]bool{id: true})
				hp.idMatched[id]++

				// We don't want any more ranges for this id
				if hp.idMatched[id] >= hp.countReqs[id] {
					deleteIdx = idx
				}

				foundId = id
				break MAIN
			}
		}
	}

	if deleteIdx != -1 {
		hp.ids = append(hp.ids[:deleteIdx], hp.ids[deleteIdx+1:]...)
	}

	return foundId
}

func (hp *hashPool) sync(r int, marked map[string]bool) {
	for len(marked) < len(hp.ids) {
		for _, id := range hp.ids {
			if _, ok := marked[id]; !ok {
				val := hp.rsources[id].Intn(hp.ranges)

				if val == r {
					marked[id] = true
				}
			}
		}
	}
}
