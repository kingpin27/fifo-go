// fan in fan out
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Task struct {
	Id int
}

type TaskQueue struct {
	Tasks []Task
	sync.Mutex
}

func (q *TaskQueue) AddTask(task Task) {
	q.Lock()
	defer q.Unlock()
	q.Tasks = append(q.Tasks, task)
}

func (q *TaskQueue) GetTask() (Task, error) {
	q.Lock()
	defer q.Unlock()
	if len(q.Tasks) == 0 {
		return Task{}, errors.New("no task")
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task, nil
}

type Worker struct {
	Id int
}

func (w *Worker) Process(task Task) {
	time.Sleep(1 * time.Second)
	log.Println("worker", w.Id, "processed task", task.Id)
}

func (w *Worker) Start(ctx *context.Context, workerWg *sync.WaitGroup, queue *TaskQueue) {
	for {
		select {
		case <-(*ctx).Done():
			log.Println("worker", w.Id, "exiting")
			workerWg.Done()
			return
		default:
			task, error := queue.GetTask()
			if error != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			w.Process(task)
		}
	}
}

type WorkerPool struct {
	Workers []*Worker
	Size    int
}

func GetWorkerPool(size int) *WorkerPool {
	workers := make([]*Worker, size)
	for i := 0; i < size; i++ {
		workers[i] = &Worker{i}
	}
	return &WorkerPool{Workers: workers, Size: size}
}

func (p *WorkerPool) Start(ctx *context.Context, workerWg *sync.WaitGroup, queue *TaskQueue) {
	for _, worker := range p.Workers {
		go worker.Start(ctx, workerWg, queue)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	workerWg := &sync.WaitGroup{}
	workerWg.Add(10)
	queue := &TaskQueue{}
	pool := GetWorkerPool(10)

	pool.Start(&ctx, workerWg, queue)

	for i := 0; i < 1000; i++ {
		queue.AddTask(Task{Id: i})
	}
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
	cancel()
	workerWg.Wait()
	log.Println("exiting")
}
