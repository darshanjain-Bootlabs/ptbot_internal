package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
)

// Request struct
type TestRequest struct {
	Script string `json:"script"`
}

// Handler with streaming
func handler(w http.ResponseWriter, r *http.Request) {
	var req TestRequest

	// Decode request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Script == "" {
		http.Error(w, "Script cannot be empty", http.StatusBadRequest)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Start k6 command
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
			line := scanner.Text()
			fmt.Fprintf(w, "data: %s\n\n", line)
			flusher.Flush()
		}
	}()

	// Stream stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintf(w, "data: ERROR: %s\n\n", line)
			flusher.Flush()
		}
	}()

	// Wait for command to finish
	cmd.Wait()

	// Send completion event
	fmt.Fprintf(w, "data: TEST COMPLETED\n\n")
	flusher.Flush()
}

func main() {
	http.HandleFunc("/run-test", handler)

	fmt.Println("Streaming server running on port 8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}