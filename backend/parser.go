package main

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"time"
)

var summaryMetricLine = regexp.MustCompile(`^(?:[✓✗]\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\.{2,}:\s*(.+)$`)
var thresholdHeaderLine = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s*$`)
var thresholdRuleLine = regexp.MustCompile(`^[✓✗]\s*'([^']+)'\s+(.+)$`)

var thresholdState = struct {
	sync.Mutex
	current map[string]string
}{
	current: map[string]string{},
}

func startMetricParser(metricChan chan StreamMessage) {
	for msg := range metricChan {
		metric, ok := parseMetricMessage(msg)
		if !ok {
			continue
		}

		saveMetric(metric)
	}
}

func parseMetricMessage(msg StreamMessage) (Metric, bool) {
	raw := strings.TrimSpace(msg.Message)
	if raw == "" {
		return Metric{}, false
	}

	clean := strings.TrimPrefix(raw, "[STDOUT] ")
	clean = strings.TrimPrefix(clean, "[STDERR] ")
	clean = strings.TrimSpace(clean)
	if clean == "" {
		return Metric{}, false
	}

	if strings.HasPrefix(raw, "{") {
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			if typ, _ := parsed["type"].(string); typ == "metric" {
				name, _ := parsed["name"].(string)
				value := ""
				if v, ok := parsed["value"]; ok {
					value = toString(v)
				}
				return Metric{
					RunID:     msg.RunID,
					Name:      name,
					Value:     value,
					Stream:    msg.Stream,
					Raw:       raw,
					CreatedAt: time.Now(),
				}, true
			}
		}
	}

	// Track threshold metric header context (e.g. http_req_duration)
	if m := thresholdHeaderLine.FindStringSubmatch(clean); len(m) == 2 {
		header := strings.TrimSpace(m[1])
		switch header {
		case "THRESHOLDS", "HTTP", "EXECUTION", "NETWORK", "TOTAL", "RESULTS":
			// section headers; ignore
		default:
			thresholdState.Lock()
			thresholdState.current[msg.RunID] = header
			thresholdState.Unlock()
		}
		return Metric{}, false
	}

	// Parse threshold rule lines (e.g. ✓ 'p(95)<500' p(95)=123.4ms)
	if m := thresholdRuleLine.FindStringSubmatch(clean); len(m) == 3 {
		thresholdState.Lock()
		header := thresholdState.current[msg.RunID]
		thresholdState.Unlock()

		name := "threshold"
		if header != "" {
			name = "threshold_" + header + "_" + m[1]
		}

		return Metric{
			RunID:     msg.RunID,
			Name:      name,
			Value:     strings.TrimSpace(m[2]),
			Stream:    msg.Stream,
			Raw:       raw,
			CreatedAt: time.Now(),
		}, true
	}

	// Parse summary metric lines with dotted alignment
	if m := summaryMetricLine.FindStringSubmatch(clean); len(m) == 3 {
		return Metric{
			RunID:     msg.RunID,
			Name:      strings.TrimSpace(m[1]),
			Value:     strings.TrimSpace(m[2]),
			Stream:    msg.Stream,
			Raw:       raw,
			CreatedAt: time.Now(),
		}, true
	}

	return Metric{}, false
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}