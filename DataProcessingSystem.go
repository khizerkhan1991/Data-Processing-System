package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Task struct {
	ID      int
	Payload int
}

type Result struct {
	TaskID     int
	Input      int
	Output     int
	ProcessedAt time.Time
}

func processTask(t Task) (Result, error) {
	// simulate computation
	time.Sleep(100 * time.Millisecond)
	out := t.Payload * t.Payload
	return Result{
		TaskID:     t.ID,
		Input:      t.Payload,
		Output:     out,
		ProcessedAt: time.Now(),
	}, nil
}

func worker(id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started\n", id)
	for task := range tasks {
		result, err := processTask(task)
		if err != nil {
			log.Printf("Worker %d encountered error for task %d: %v\n", id, task.ID, err)
			continue
		}
		log.Printf("Worker %d processed task %d input=%d output=%d\n",
			id, result.TaskID, result.Input, result.Output)
		results <- result
	}
	log.Printf("Worker %d completed\n", id)
}

func writeResultsToFile(results <-chan Result, fileName string, done chan<- struct{}) {
	defer close(done)

	file, err := os.Create(fileName)
	if err != nil {
		log.Printf("Error creating results file: %v\n", err)
		return
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			log.Printf("Error closing file: %v\n", cerr)
		}
	}()

	for r := range results {
		line := fmt.Sprintf("Result{taskId=%d, input=%d, output=%d, processedAt=%s}\n",
			r.TaskID, r.Input, r.Output, r.ProcessedAt.Format(time.RFC3339))
		if _, err := file.WriteString(line); err != nil {
			log.Printf("Error writing result to file: %v\n", err)
			return
		}
	}

	log.Println("Results successfully written to file:", fileName)
}

func main() {
	const numWorkers = 4
	const numTasks = 20

	log.Println("Starting Data Processing System (Go)")

	tasks := make(chan Task)
	results := make(chan Result)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, tasks, results, &wg)
	}

	// Start result writer
	done := make(chan struct{})
	go writeResultsToFile(results, "go_results.txt", done)

	// Send tasks
	go func() {
		for i := 0; i < numTasks; i++ {
			tasks <- Task{ID: i, Payload: i + 1}
		}
		close(tasks) // signal no more tasks
	}()

	// Wait for workers and then close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Wait until result writer finishes
	<-done

	log.Println("Data Processing System (Go) finished.")
}
