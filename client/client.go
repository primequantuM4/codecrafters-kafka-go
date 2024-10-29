package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	// Connect to the TCP server
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		fmt.Println("Error connecting to server:", err.Error())
		os.Exit(1)
	}
	go sendCompleteRequest(conn)
	select {}
}

func sendCompleteRequest(conn net.Conn) {

	defer conn.Close()

	length := int32(8)
	requestApiKey := int16(18)
	requestApiVersion := int16(1)
	correlationId := int32(1234)

	for {
		// Increment correlationId for each request
		correlationId++

		// Create a new buffer for each request to avoid data overlap
		buf := new(bytes.Buffer)

		// Write fields to buffer
		if err := binary.Write(buf, binary.BigEndian, length); err != nil {
			fmt.Println("Error writing length:", err)
			os.Exit(1)
		}
		if err := binary.Write(buf, binary.BigEndian, requestApiKey); err != nil {
			fmt.Println("Error writing request API key:", err)
			os.Exit(1)
		}
		if err := binary.Write(buf, binary.BigEndian, requestApiVersion); err != nil {
			fmt.Println("Error writing API version:", err)
			os.Exit(1)
		}
		if err := binary.Write(buf, binary.BigEndian, correlationId); err != nil {
			fmt.Println("Error writing correlation ID:", err)
			os.Exit(1)
		}

		// Send the request
		fmt.Printf("Sending request with correlationId: %d\n", correlationId)
		if _, err := conn.Write(buf.Bytes()); err != nil {
			fmt.Println("Error sending data:", err)
			os.Exit(1)
		}

		// Wait for the response
		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			fmt.Println("Error reading response:", err)
			os.Exit(1)
		}

		// Process the response
		recBuff := bytes.NewReader(response[:n])
		var respLen int32
		var respCorrId int32

		// Read length and correlationId from the response
		binary.Read(recBuff, binary.BigEndian, &respLen)
		binary.Read(recBuff, binary.BigEndian, &respCorrId)

		fmt.Printf("Received response with correlationId: %d\n", respCorrId)

		// Optional: wait before sending the next request
		time.Sleep(1 * time.Second)
	}
}

func sendPartialData(conn net.Conn) {
	length := int32(4)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, length)
	binary.Write(buf, binary.BigEndian, int16(18)) // ApiKey only, no version or correlationId

	_, err := conn.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error sending partial data:", err)
	}
}
