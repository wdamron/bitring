// package bitring provides a bitmap ring-buffer which tracks the state of windowed out-of-order processing
// over a sequence of logical offsets. The initially marked offset can be any non-negative integer.
package bitring

import (
	"math/bits"
)

const (
	minCapacity = 64
)

func pow2BitCapacity(bitCapacity int) uint {
	if bitCapacity < minCapacity {
		return minCapacity
	}
	return 1 << uint(1+(63-bits.LeadingZeros64(uint64(bitCapacity-1))))
}

func getBit(bitmap []uint64, pos uint) bool {
	return bitmap[pos/64]&(1<<(pos%64)) != 0
}

func setBit(bitmap []uint64, pos uint) {
	bitmap[pos/64] |= 1 << (pos % 64)
}

func tryClearBit(bitmap []uint64, pos uint) bool {
	blockIndex, bit := pos/64, uint64(1<<(pos%64))
	block := bitmap[blockIndex]
	bitmap[blockIndex] = block &^ bit
	return block&bit != 0
}

// Ring is a bitmap ring-buffer which tracks the state of windowed out-of-order processing
// over a sequence of logical offsets. The initially marked offset can be any non-negative integer.
type Ring struct {
	bitmap          []uint64
	tailBit         uint
	lowestMarked    int
	highestMarked   int
	lowestPending   int
	highestPending  int
	lowestComplete  int
	highestComplete int
	numCommittable  int
}

// New creates a ring with initial capacity to store at least bitCapacity bits.
func New(bitCapacity int) *Ring {
	return &Ring{
		bitmap:          make([]uint64, pow2BitCapacity(bitCapacity)/64),
		lowestMarked:    -1,
		highestMarked:   -1,
		lowestPending:   -1,
		highestPending:  -1,
		lowestComplete:  -1,
		highestComplete: -1,
	}
}

// Reset the state of r with initial capacity to store at least bitCapacity bits.
func (r *Ring) Reset(bitCapacity int) {
	r.resetBitmap(bitCapacity)
	r.lowestMarked = -1
	r.highestMarked = -1
	r.lowestPending = -1
	r.highestPending = -1
	r.lowestComplete = -1
	r.highestComplete = -1
}

func (r *Ring) resetBitmap(bitCapacity int) {
	if initialBits := pow2BitCapacity(bitCapacity); r.bitCapacity() != initialBits {
		r.bitmap = make([]uint64, initialBits/64)
	} else {
		for i := range r.bitmap {
			r.bitmap[i] = 0
		}
	}
}

// LowestMarkedOffset returns the first marked offset.
func (r *Ring) LowestMarkedOffset() int { return r.lowestMarked }

// HighestMarkedOffset returns the highest offset which has been marked pending or complete.
func (r *Ring) HighestMarkedOffset() int { return r.highestMarked }

// LowestPendingOffset returns the lowest offset which has been marked pending but not complete.
func (r *Ring) LowestPendingOffset() int { return r.lowestPending }

// HighestPendingOffset returns the highest offset which has been marked pending but not complete.
func (r *Ring) HighestPendingOffset() int {
	if r.highestPending >= 0 {
		return r.highestPending
	}
	r.highestPending = r.findHighestPending()
	return r.highestPending
}

// LowestCompleteOffset returns the lowest offset which has been marked complete.
func (r *Ring) LowestCompleteOffset() int {
	if r.lowestComplete >= 0 {
		return r.lowestComplete
	}
	r.lowestComplete = r.findLowestComplete()
	return r.lowestComplete
}

// HighestCompleteOffset returns the highest offset which has been marked complete.
func (r *Ring) HighestCompleteOffset() int { return r.highestComplete }

// CommittableOffset returns the lowest offset which has been marked complete with
// no lower offsets which have not been marked complete.
func (r *Ring) CommittableOffset() int {
	if r.lowestPending < 0 {
		return r.highestMarked
	}
	return r.lowestPending - 1
}

// CommittableCount returns the number of offsets which have marked complete but can
// only be committed once a lower pending offset is marked complete.
func (r *Ring) CommittableCount() int { return r.numCommittable }

// PendingRangeSize returns the number of offsets between the lowest offset which has been marked
// pending but not complete and the highest offset which has been marked pending or complete.
func (r *Ring) PendingRangeSize() int {
	if r.lowestPending < 0 {
		return 0
	}
	return r.highestMarked + 1 - r.lowestPending
}

// CompleteRangeSize returns the number of offsets between the lowest offset which has been
// marked pending but not complete and the highest offset which has been marked complete.
func (r *Ring) CompleteRangeSize() int {
	if r.lowestPending < 0 || r.highestComplete < 0 {
		return 0
	}
	return r.highestComplete + 1 - r.lowestPending
}

// MarkPending marks offset as pending.
func (r *Ring) MarkPending(offset int) {
	if offset > r.highestMarked {
		r.highestMarked = offset
	}
	if r.lowestMarked >= 0 {
		if r.highestPending < offset {
			r.highestPending = offset
		}
		if r.lowestPending < 0 || r.lowestPending > offset {
			r.lowestPending = offset
		}
		return
	}
	r.lowestMarked = offset
	r.lowestPending = offset
}

// MarkComplete marks offset as complete.
func (r *Ring) MarkComplete(offset int) {
	rel := offset - r.lowestPending
	if rel < 0 {
		return // already committed
	}
	var pos uint
	if uint(rel) < r.bitCapacity() {
		pos = r.wrapForward(r.tailBit + uint(rel))
		if getBit(r.bitmap, pos) {
			return // already marked
		}
	} else {
		r.resize(rel+1, r.CompleteRangeSize())
		pos = uint(rel)
	}
	setBit(r.bitmap, pos)
	r.numCommittable++
	// lowestPending is adjusted below, if the lowest pending offset is being marked complete.
	r.highestPending = -1 // unset cached offset
	r.lowestComplete = -1 // unset cached offset
	if r.highestComplete < offset {
		r.highestComplete = offset
	}
	if pos != r.tailBit {
		return
	}
	// When the lowest pending offset is being marked complete, coalesce with subsequent offsets
	// which are already marked complete:
	for tryClearBit(r.bitmap, r.tailBit) {
		r.tailBit = r.wrapForward(r.tailBit + 1)
		r.numCommittable--
		r.lowestPending++
	}
	// Reset internal state if all offsets are committable:
	if r.lowestPending > r.highestMarked {
		r.lowestPending = -1
		r.tailBit = 0
		r.resetBitmap(minCapacity)
		return
	}
	rangeSize := r.CompleteRangeSize()
	// If the bitmap is less than 25% full, resize it to be approximately 50% full:
	if uint(rangeSize*4) < r.bitCapacity() {
		r.resize(rangeSize*2, rangeSize)
	}
}

func (r *Ring) bitCapacity() uint { return uint(len(r.bitmap)) * 64 }

// Wrap around to the start if necessary (this assumes the capacity is always a power of 2).
func (r *Ring) wrapForward(bitPosition uint) uint {
	return bitPosition & (r.bitCapacity() - 1)
}

// Wrap around to the end if necessary (this assumes the capacity is always a power of 2).
func (r *Ring) wrapBack1(bitPosition uint) uint {
	bitCap := r.bitCapacity()
	return (bitCap + bitPosition - 1) & (bitCap - 1)
}

func (r *Ring) resize(bitCapacity, completeRangeSize int) {
	existing := r.bitmap
	r.bitmap = make([]uint64, pow2BitCapacity(bitCapacity)/64)
	rangeBlocks := completeRangeSize / 64
	if completeRangeSize%64 != 0 || r.tailBit%64 != 0 {
		rangeBlocks++
	}
	for copied, block := 0, r.tailBit/64; copied < rangeBlocks; copied, block = copied+1, (block+1)&(uint(len(existing))-1) {
		r.bitmap[copied] = existing[block]
	}
	r.tailBit = 0
}

func (r *Ring) findHighestPending() int {
	if r.lowestPending < 0 {
		return -1
	}
	if r.highestComplete < r.highestMarked {
		return r.highestMarked
	}
	offset := r.highestComplete
	for i := r.wrapForward(r.tailBit + uint(r.highestComplete-r.lowestPending)); i != r.tailBit && getBit(r.bitmap, i); i = r.wrapBack1(i) {
		offset--
	}
	return offset
}

func (r *Ring) findLowestComplete() int {
	if r.lowestPending < 0 || r.highestComplete < 0 {
		return -1
	}
	offset, end := r.lowestPending, r.wrapForward(r.tailBit+uint(r.highestComplete-r.lowestPending))
	for i := r.tailBit; i != end && !getBit(r.bitmap, i); i = r.wrapForward(i + 1) {
		offset++
	}
	return offset
}
