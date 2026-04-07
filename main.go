package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
)

// Request struct (user sends full k6 script)
type TestRequest struct {
	Script string `json:"script"`
}

// Function to run k6 with user-provided script
func runK6Test(script string) (string, error) {
	cmd := exec.Command("k6", "run", "-")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}

	// Send script to k6 via stdin
	go func() {
		defer stdin.Close()
		stdin.Write([]byte(script))
	}()

	// Capture output (logs + metrics)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// HTTP handler
func handler(w http.ResponseWriter, r *http.Request) {
	var req TestRequest

	// Decode request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Basic validation
	if req.Script == "" {
		http.Error(w, "Script cannot be empty", http.StatusBadRequest)
		return
	}

	resultChan := make(chan string)

	// Run test in goroutine (runner)
	go func() {
		output, err := runK6Test(req.Script)
		if err != nil {
			resultChan <- fmt.Sprintf("Error: %v\nOutput: %s", err, output)
			return
		}
		resultChan <- output
	}()

	// Wait for result (blocking)
	result := <-resultChan

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"result": result,
	})
}

func main() {
	http.HandleFunc("/run-test", handler)

	fmt.Println("Server running on port 8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}