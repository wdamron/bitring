package bitring

import (
	"math/rand"
	"testing"
)

func TestRing(t *testing.T) {
	const (
		messages     = 1500
		permutations = 10000
	)

	bitmap := make([]uint64, 2)
	for i := uint(0); i < 128; i++ {
		setBit(bitmap, i)
		if !getBit(bitmap, i) {
			t.Fatalf("failed to set/get bit at %v", i)
		}
	}

	switch {
	case pow2BitCapacity(15) != 64, pow2BitCapacity(64) != 64,
		pow2BitCapacity(127) != 128, pow2BitCapacity(128) != 128,
		pow2BitCapacity(511) != 512, pow2BitCapacity(512) != 512:
		t.Fatalf("wrong calculated capacity")
	}

	// forward completion order
	ring := New(64)
	for i := 0; i < messages; i++ {
		ring.MarkPending(i)
	}
	for i := 0; i < messages; i++ {
		ring.MarkComplete(i)
	}
	if ring.CommittableOffset() != messages-1 {
		t.Fatalf("committable offset: %v", ring.CommittableOffset())
	}

	// reverse completion order
	ring.Reset(64)
	for i := 0; i < messages; i++ {
		ring.MarkPending(i)
	}
	for i := messages - 1; i >= 0; i-- {
		ring.MarkComplete(i)
	}
	if ring.CommittableOffset() != messages-1 {
		t.Fatalf("committable offset: %v", ring.CommittableOffset())
	}

	// random completion order
	shuffled := make([]int, messages)
	for perm := 0; perm < permutations; perm++ {
		rand.Shuffle(messages, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

		ring.Reset(64)
		for i := 0; i < messages; i++ {
			ring.MarkPending(i)
		}
		for i := range shuffled {
			ring.MarkComplete(i)
		}
		if ring.CommittableOffset() != messages-1 {
			t.Fatalf("committable offset: %v", ring.CommittableOffset())
		}
	}
}

func BenchmarkCoalescedCommittableOffsets(b *testing.B) {
	const messages = 1500
	ring, n := New(minCapacity), b.N
	b.ResetTimer()
	for i := 0; i < n; i++ {
		// reverse completion order
		ring.Reset(minCapacity)
		for i := 0; i < messages; i++ {
			ring.MarkPending(i)
		}
		for i := messages - 1; i >= 0; i-- {
			ring.MarkComplete(i)
		}
	}
}
