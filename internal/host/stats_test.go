package host

import (
	"testing"
)

func TestSummarizeCPUUsageDerivesAggregateFromPerCore(t *testing.T) {
	usedPercent, usedCores := summarizeCPUUsage([]float64{0, 50, 100, 25}, 4)

	if usedPercent != 43.75 {
		t.Fatalf("used percent=%f want 43.75", usedPercent)
	}

	if usedCores != 1.75 {
		t.Fatalf("used cores=%f want 1.75", usedCores)
	}
}
