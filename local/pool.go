package local

import "sync"

type Task func()

type Pool struct {
	numWorkers int
	tasks      chan Task
	once       sync.Once
	wg         sync.WaitGroup
}

func NewPool(numWorkers int) *Pool {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	return &Pool{
		numWorkers: numWorkers,
		tasks:      make(chan Task, numWorkers),
	}
}

func (p *Pool) Start() {
	p.once.Do(func() {
		for i := 0; i < p.numWorkers; i++ {
			p.wg.Go(func() {
				for task := range p.tasks {
					if task != nil {
						task()
					}
				}
			})
		}
	})
}

func (p *Pool) Submit(task Task) {
	p.tasks <- task
}

func (p *Pool) Close() {
	close(p.tasks)
	p.wg.Wait()
}
