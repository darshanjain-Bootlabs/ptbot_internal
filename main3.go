package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
)

// Request struct
type TestRequest struct {
	VUs    int    `json:"vus"`
	Script string `json:"script"`
}

// 🔥 Global resource tracking
var currentVUs = 0
var maxVUs = 2000
var mu sync.Mutex

func handler(w http.ResponseWriter, r *http.Request) {
	var req TestRequest

	// Decode request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Script == "" || req.VUs <= 0 {
		http.Error(w, "Invalid script or VUs", http.StatusBadRequest)
		return
	}

	// 🔐 Admission control (VU-based)
	mu.Lock()
	if currentVUs+req.VUs > maxVUs {
		mu.Unlock()
		http.Error(w, "container limit reached", http.StatusTooManyRequests)
		return
	}
	currentVUs += req.VUs
	fmt.Println("Allocated VUs:", req.VUs, " | Current VUs:", currentVUs)
	mu.Unlock()

	// 🔄 Release VUs after completion
	defer func() {
		mu.Lock()
		currentVUs -= req.VUs
		fmt.Println("Released VUs:", req.VUs, " | Current VUs:", currentVUs)
		mu.Unlock()
	}()

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", 500)
		return
	}

	// Start k6 process
	cmd := exec.Command("k6", "run", "-")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, "Error getting stdout", 500)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, "Error getting stderr", 500)
		return
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		http.Error(w, "Error getting stdin", 500)
		return
	}

	// Send script to k6
	go func() {
		defer stdin.Close()
		stdin.Write([]byte(req.Script))
	}()

	// Start process
	if err := cmd.Start(); err != nil {
		http.Error(w, "Failed to start k6", 500)
		return
	}

	// Stream stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fmt.Fprintf(w, "data: %s\n\n", scanner.Text())
			flusher.Flush()
		}
	}()

	// Stream stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Fprintf(w, "data: ERROR: %s\n\n", scanner.Text())
			flusher.Flush()
		}
	}()

	// Wait for completion
	cmd.Wait()

	// Final message
	fmt.Fprintf(w, "data: TEST COMPLETED\n\n")
	flusher.Flush()
}

func main() {
	http.HandleFunc("/run-test", handler)

	fmt.Println("Server running on port 8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}