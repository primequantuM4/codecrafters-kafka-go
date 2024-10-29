package main

import (
	"fmt"
	"testing"
)

func TestResponseBodyConstruction(t *testing.T) {
	response := constructResponse(12345) // Pass a mock correlation ID

	if response.CorrelationId != 12345 {
		t.Error("Expected CorrelationId to be 12345")
	}
	if len(response.Versions) != 2 { // Change to expected count
		t.Errorf("Expected 1 API key, got %d", len(response.Versions))
	}
	if response.ThrottleTimeMs != 0 { // Adjust based on your logic
		t.Errorf("Expected ThrottleTimeMs to be 0, got %d", response.ThrottleTimeMs)
	}
}

func TestAPIVersions(t *testing.T) {
	response := constructResponse(12345) // Pass a mock correlation ID

	apiVersion := []ApiVersion{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		{ApiKey: 1, MinVersion: 0, MaxVersion: 16},
	}

	for i := 0; i < len(response.Versions); i++ {
		if apiVersion[i] != response.Versions[i] {
			fmt.Println("apiversion", apiVersion[i])
			fmt.Println("fetch ap vesion", response.Versions[i])
			t.Errorf("Expected apiVersion to be 18,0,4 or 1,0,16")
		}
	}

}
