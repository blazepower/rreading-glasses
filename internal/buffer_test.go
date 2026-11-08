package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAccumulateEdges(t *testing.T) {
	buf := &edgebuf{}
	assert.Equal(t, 0, buf.len())

	producer := make(chan edge)
	consumer := accumulate(producer, buf)

	producer <- edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(1))}
	producer <- edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(2), int64(3))}
	producer <- edge{kind: workEdge, parentID: 100, childIDs: newSet(int64(4))}
	// We unblock as soon as a value is sent down the producer channel but
	// before the buffer is updated. Sleep to allow the other goroutine to allow it
	// to actually push the value into the buffer. Racy but it works for now.
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 4, buf.len())

	e := <-consumer
	assert.Equal(t, edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(1), int64(2), int64(3))}, e)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, buf.len())

	e = <-consumer
	assert.Equal(t, edge{kind: workEdge, parentID: 100, childIDs: newSet(int64(4))}, e)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, buf.len())

	producer <- edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(5), int64(6))}
	producer <- edge{kind: authorEdge, parentID: 200, childIDs: newSet(int64(7))}
	producer <- edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(8))}
	producer <- edge{kind: authorEdge, parentID: 300, childIDs: newSet(int64(9))}
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 5, buf.len())

	e = <-consumer
	assert.Equal(t, edge{kind: authorEdge, parentID: 100, childIDs: newSet(int64(5), int64(6), int64(8))}, e)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 2, buf.len())

	e = <-consumer
	assert.Equal(t, edge{kind: authorEdge, parentID: 200, childIDs: newSet(int64(7))}, e)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, buf.len())

	e = <-consumer
	assert.Equal(t, edge{kind: authorEdge, parentID: 300, childIDs: newSet(int64(9))}, e)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, buf.len())

	close(producer)

	_, ok := <-consumer
	assert.False(t, ok)
}

func TestAccumulateSlice(t *testing.T) {
	buf := slicebuffer[int]{}
	producer := make(chan int)
	consumer := accumulate(producer, &buf)

	// Test this case where we consume before producing.
	go func() {
		time.Sleep(time.Second)
		producer <- -1
	}()
	x := <-consumer
	assert.Equal(t, -1, x)

	producer <- 1
	producer <- 2
	producer <- 3

	n := <-consumer
	assert.Equal(t, 1, n)
	n = <-consumer
	assert.Equal(t, 2, n)
	n = <-consumer
	assert.Equal(t, 3, n)

	close(producer)

	_, ok := <-consumer
	assert.False(t, ok)
}

type diagnosticPopBuffer struct {
	edgebuf
	popStarted  chan struct{}
	popAllowed  chan struct{}
	popFinished chan struct{}
}

func (b *diagnosticPopBuffer) pop() edge {
	close(b.popStarted)
	<-b.popAllowed
	e := b.edgebuf.pop()
	close(b.popFinished)
	return e
}

func TestAccumulatePublishesFinalQueueSize(t *testing.T) {
	metrics := newControllerMetrics(nil)
	buf := &diagnosticPopBuffer{
		edgebuf:     edgebuf{onSizeChanged: metrics.denormWaitingSet},
		popStarted:  make(chan struct{}),
		popAllowed:  make(chan struct{}),
		popFinished: make(chan struct{}),
	}
	producer := make(chan edge)
	consumer := accumulate(producer, buf)

	producer <- edge{kind: workEdge, parentID: 1, childIDs: newSet(int64(2))}
	delivered := <-consumer
	<-buf.popStarted

	before := metrics.denormWaitingGet()
	close(buf.popAllowed)
	<-buf.popFinished

	assert.Equal(t, int64(1), delivered.parentID)
	assert.Equal(t, 0, buf.len())
	assert.Equal(t, 1.0, before)
	assert.Equal(t, 0.0, metrics.denormWaitingGet())
	t.Logf("edge delivered; queue=%d, metric=%g after final dequeue", buf.len(), metrics.denormWaitingGet())

	close(producer)
	_, ok := <-consumer
	assert.False(t, ok)
}
