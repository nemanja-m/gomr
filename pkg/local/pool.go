package local

import "sync"

type Task func()

type Pool struct {
	numWorkers int
	tasks      chan Task
	wg         sync.WaitGroup
}

func NewPool(numWorkers int) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		tasks:      make(chan Task),
	}
}

func (p *Pool) Start() {
	for range p.numWorkers {
		p.wg.Go(func() {
			for task := range p.tasks {
				task()
			}
		})
	}
}

func (p *Pool) Submit(task Task) {
	p.tasks <- task
}

func (p *Pool) Close() {
	close(p.tasks)
	p.wg.Wait()
}
