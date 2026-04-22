package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting Backend with Temporal Workflow Engine...")

	// Init DB
	initDB()

	// Initialize Temporal worker
	if err := InitTemporalWorker(); err != nil {
		fmt.Println("Failed to initialize Temporal worker:", err)
		os.Exit(1)
	}

	// Start Temporal worker in background
	if err := StartTemporalWorker(); err != nil {
		fmt.Println("Failed to start Temporal worker:", err)
		os.Exit(1)
	}

	// Graceful shutdown handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in goroutine
	go func() {
		http.HandleFunc("/run-test", handleRunTestWithWorkflow)
		http.HandleFunc("/health", handleHealth)

		port := os.Getenv("BACKEND_PORT")
		if port == "" {
			port = "8081"
		}

		fmt.Println("Backend running on port", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			fmt.Println("Server error:", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("Shutting down...")
	StopTemporalWorker()
	fmt.Println("Backend stopped")
}

// handleHealth is a simple health check endpoint
func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status": "healthy"}`)
}