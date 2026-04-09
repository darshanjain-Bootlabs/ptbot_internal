package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func createRunLogFile(runID string) error {
	fileName := sanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	if err := os.MkdirAll("results", 0755); err != nil {
		return err
	}

	filePath := filepath.Join("results", fmt.Sprintf("%s.txt", fileName))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	return file.Close()
}

func startFileWriter(logChan chan StreamMessage) {
	for msg := range logChan {
		line := strings.TrimSpace(msg.Message)
		if line == "" {
			continue
		}

		fileName := sanitizeRunID(msg.RunID)
		if fileName == "" {
			fileName = "run_unknown"
		}

		filePath := filepath.Join("results", fmt.Sprintf("%s.txt", fileName))
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("file writer error:", err)
			continue
		}

		_, _ = file.WriteString(fmt.Sprintf("[%s] [%s] %s\n", msg.RunID, msg.Stream, line))
		_ = file.Close()
	}
}

func sanitizeRunID(runID string) string {
	if runID == "" {
		return ""
	}

	var b strings.Builder
	for _, r := range runID {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('_')
	}

	return b.String()
}