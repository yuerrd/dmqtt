package broker

import "sync"

type PacketIDAllocator struct {
	mu      sync.Mutex
	counter uint16
}

func NewPacketIDAllocator() *PacketIDAllocator {
	return &PacketIDAllocator{}
}

func (a *PacketIDAllocator) Next() uint16 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counter++
	if a.counter == 0 {
		a.counter = 1
	}
	return a.counter
}
