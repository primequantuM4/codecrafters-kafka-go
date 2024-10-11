package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type DataBody struct {
	length            int32
	correlationId     int32
	requestApiVersion int16
	requestApiKey     int16
}

type ResponseBody struct {
	length         int32
	correlationId  int32
	errorCode      int16
	numOfApiKeys   int8
	apiKey         int16
	minVersion     int16
	maxVersion     int16
	throttleTimeMs int32
}

func parseData(connection net.Conn) (DataBody, error) {
	var dataBody DataBody

	buffer := make([]byte, 1024)
	n, err := connection.Read(buffer)
	if err != nil {
		return dataBody, err
	}

	newBuffer := buffer[:n]
	fmt.Println("Byte array is: ", newBuffer)
	reader := bytes.NewReader(newBuffer)

	binary.Read(reader, binary.BigEndian, &dataBody.length)
	binary.Read(reader, binary.BigEndian, &dataBody.requestApiKey)
	binary.Read(reader, binary.BigEndian, &dataBody.requestApiVersion)
	binary.Read(reader, binary.BigEndian, &dataBody.correlationId)

	fmt.Println("Current version is: ", dataBody.requestApiVersion)
	return dataBody, nil
}

func writeBytes[T int8 | int16 | int32](resField T, buff *bytes.Buffer) {
	err := binary.Write(buff, binary.BigEndian, resField)
	if err != nil {
		fmt.Println("Error while writing to buffer")
		os.Exit(1)
	}

}

func handleRequest(conn net.Conn, db DataBody) {

	var responseBody ResponseBody

	responseBody.length = 19
	responseBody.correlationId = db.correlationId

	if db.requestApiVersion < 0 || db.requestApiVersion > 4 {
		responseBody.errorCode = 35
	} else {
		responseBody.errorCode = 0
	}
	responseBody.apiKey = 18
	responseBody.minVersion = 0
	responseBody.maxVersion = 4
	responseBody.numOfApiKeys = 2
	responseBody.throttleTimeMs = 0

	buff := new(bytes.Buffer)

	if responseBody.errorCode == 35 {
		writeBytes(int32(6), buff)
		writeBytes(responseBody.correlationId, buff)
		writeBytes(responseBody.errorCode, buff)

		conn.Write(buff.Bytes())
		os.Exit(0)
	}

	writeBytes(responseBody.length, buff)
	writeBytes(responseBody.correlationId, buff)
	writeBytes(responseBody.errorCode, buff)
	writeBytes(responseBody.numOfApiKeys, buff)
	writeBytes(responseBody.apiKey, buff)
	writeBytes(responseBody.minVersion, buff)
	writeBytes(responseBody.maxVersion, buff)

	// tagged fields
	writeBytes(int8(0), buff)
	writeBytes(responseBody.throttleTimeMs, buff)
	writeBytes(int8(0), buff)

	fmt.Println("Recieved correlation id of: ", responseBody.correlationId)

	_, err := conn.Write(buff.Bytes())

	if err != nil {
		fmt.Println("Could not send the correlation id")
		os.Exit(1)
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	} else {
		fmt.Println("Listening on port", l.Addr().String())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		fmt.Println("Client connected from: ", conn.RemoteAddr().String())
		go func(conn net.Conn) {
			for {
				db, err := parseData(conn)
				if err != nil {
					fmt.Println("Could not read connection")
					return
				}

				handleRequest(conn, db)
				defer conn.Close()
			}

		}(conn)
	}

}
