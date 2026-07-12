package host

import (
	"sync"
	"testing"
)

func TestLogQueueBoundsEntryCount(t *testing.T) {
	sub := &hostLogSubscription{}
	sub.cond = sync.NewCond(&sub.mu)

	for range maxQueuedLogItems * 4 {
		sub.Enqueue([]byte("x"))
	}

	if queued := len(sub.queue) - sub.head; queued > maxQueuedLogItems {
		t.Fatalf("queued items = %d, max %d", queued, maxQueuedLogItems)
	}
}
