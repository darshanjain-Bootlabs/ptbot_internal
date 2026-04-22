package main

import (
	"context"
	"fmt"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// TemporalWorker handles workflow and activity execution
var temporalClient client.Client
var temporalWorker worker.Worker

const (
	TemporalTaskQueue = "k6-loadtest-queue"
	TemporalNamespace = "default"
)

// InitTemporalWorker initializes the Temporal client and worker
func InitTemporalWorker() error {
	var err error

	// Create Temporal client
	temporalClient, err = client.Dial(client.Options{})
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
		return err
	}

	fmt.Println("Connected to Temporal server")

	// Create worker
	temporalWorker = worker.New(temporalClient, TemporalTaskQueue, worker.Options{})

	// Register workflow
	temporalWorker.RegisterWorkflow(LoadTestWorkflow)

	// Register activities
	temporalWorker.RegisterActivity(ActivityCreateRun)
	temporalWorker.RegisterActivity(ActivityCreateLogFile)
	temporalWorker.RegisterActivity(ActivityCallRunner)
	temporalWorker.RegisterActivity(ActivityProcessStream)
	temporalWorker.RegisterActivity(ActivityWriteToLogFile)
	temporalWorker.RegisterActivity(ActivityExtractMetrics)
	temporalWorker.RegisterActivity(ActivitySaveMetricsToDb)
	temporalWorker.RegisterActivity(ActivitySaveRunLogFile)
	temporalWorker.RegisterActivity(ActivityUpdateRunStatus)

	fmt.Println("Temporal worker initialized and activities registered")
	return nil
}

// StartTemporalWorker starts the worker in a background goroutine
func StartTemporalWorker() error {
	err := temporalWorker.Start()
	if err != nil {
		log.Fatalf("Failed to start Temporal worker: %v", err)
		return err
	}

	fmt.Println("Temporal worker started successfully")
	return nil
}

// StopTemporalWorker gracefully stops the worker and closes the client
func StopTemporalWorker() {
	temporalWorker.Stop()
	temporalClient.Close()
	fmt.Println("Temporal worker stopped")
}

// StartLoadTestWorkflow starts a new load test workflow execution
func StartLoadTestWorkflow(req RunRequest) (string, error) {
	if temporalClient == nil {
		return "", fmt.Errorf("temporal client not initialized")
	}

	// Create workflow options
	options := client.StartWorkflowOptions{
		ID:        req.RunID,
		TaskQueue: TemporalTaskQueue,
	}

	// Start workflow
	we, err := temporalClient.ExecuteWorkflow(context.Background(), options, LoadTestWorkflow, req)
	if err != nil {
		log.Printf("Failed to start workflow: %v", err)
		return "", err
	}

	fmt.Printf("Workflow started with ID: %s\n", we.GetID())
	return we.GetID(), nil
}
