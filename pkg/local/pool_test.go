package local

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPool_TaskExecution(t *testing.T) {
	p := NewPool(2)
	p.Start()

	var called int32
	p.Submit(func() { atomic.AddInt32(&called, 1) })
	p.Submit(func() { atomic.AddInt32(&called, 1) })

	// close and wait for tasks
	p.Close()
	require.Equal(t, int32(2), atomic.LoadInt32(&called))
}

func TestPool_CloseWaitsForLongTask(t *testing.T) {
	p := NewPool(1)
	p.Start()

	var done int32
	p.Submit(func() {
		time.Sleep(50 * time.Millisecond)
		atomic.StoreInt32(&done, 1)
	})

	// Close should wait for the running task to finish
	p.Close()
	require.Equal(t, int32(1), atomic.LoadInt32(&done))
}

func TestPool_SubmitAfterClosePanics(t *testing.T) {
	p := NewPool(1)
	p.Start()
	p.Close()

	// Submitting after close will panic; ensure it does and recover
	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		p.Submit(func() {})
	}()
	require.True(t, didPanic)
}
