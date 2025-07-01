package state

import "sync"

type Condvar struct {
	sync.Mutex
	notified bool
	cond     sync.Cond
}

func NewCondvar() *Condvar {
	c := &Condvar{}
	c.cond = *sync.NewCond(c)
	return c
}

func (cv *Condvar) Wait() {
	cv.Lock()
	for !cv.notified {
		cv.cond.Wait()
	}
	cv.Unlock()
}

func (cv *Condvar) Signal() {
	cv.Lock()
	cv.notified = true
	cv.Unlock()
	cv.cond.Signal()
}
