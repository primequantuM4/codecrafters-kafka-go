package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// ERROR CODE TYPES
const UNSUPPORTED_VERSION = int16(35)
const NO_ERROR_CODE = int16(0)

// VERSION TYPES
const MIN_VERSION = int16(0)
const MAX_VERSION = int16(16)
const FETCH_REQUEST_VERSION = int16(16)

const TAG_BUFFER = int8(0)

type DataBody struct {
	Length            int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (av *ApiVersion) Encode(buff *bytes.Buffer) {
	writeBytes(av.ApiKey, buff)
	writeBytes(av.MinVersion, buff)
	writeBytes(av.MaxVersion, buff)
	writeBytes(TAG_BUFFER, buff)
}

type ResponseBody struct {
	CorrelationId  int32
	ErrorCode      int16
	NumOfApiKeys   int8
	Versions       []ApiVersion
	ThrottleTimeMs int32
}

func (rb *ResponseBody) Encode(buff *bytes.Buffer) {
	writeBytes(rb.CorrelationId, buff)
	writeBytes(rb.ErrorCode, buff)

	writeBytes(rb.NumOfApiKeys+1, buff)

	for _, version := range rb.Versions {
		version.Encode(buff)
	}

	writeBytes(rb.ThrottleTimeMs, buff)
	writeBytes(TAG_BUFFER, buff)
}

type FetchResponse struct {
	CorrelationId  int32
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Response       int8
}

func (fr *FetchResponse) Encode(buff *bytes.Buffer) {

	//Response Header
	writeBytes(fr.CorrelationId, buff)
	writeBytes(TAG_BUFFER, buff)
	//Response Body
	writeBytes(fr.ThrottleTimeMs, buff)
	writeBytes(fr.ErrorCode, buff)
	writeBytes(fr.SessionId, buff)
	writeBytes(fr.Response, buff)
	writeBytes(TAG_BUFFER, buff)
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

	binary.Read(reader, binary.BigEndian, &dataBody.Length)
	binary.Read(reader, binary.BigEndian, &dataBody.RequestApiKey)
	binary.Read(reader, binary.BigEndian, &dataBody.RequestApiVersion)
	binary.Read(reader, binary.BigEndian, &dataBody.CorrelationId)

	fmt.Println("Current version is: ", dataBody.RequestApiVersion)
	return dataBody, nil
}

func constructResponse(correlationId int32) ResponseBody {
	responseBody := ResponseBody{
		CorrelationId:  correlationId,
		ErrorCode:      NO_ERROR_CODE,
		NumOfApiKeys:   2,
		ThrottleTimeMs: 0,
	}

	responseBody.Versions = append(
		responseBody.Versions,
		ApiVersion{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		ApiVersion{ApiKey: 1, MinVersion: 0, MaxVersion: 16},
	)

	return responseBody
}

func constructFetchResponse(correlationId int32) FetchResponse {
	fetchResponse := FetchResponse{
		CorrelationId:  correlationId,
		ThrottleTimeMs: 0,
		ErrorCode:      NO_ERROR_CODE,
		SessionId:      0,
	}

	return fetchResponse
}

func sendErrorResponse(conn net.Conn, correlationId int32) {
	buff := new(bytes.Buffer)
	responseBody := ResponseBody{
		CorrelationId: correlationId,
		ErrorCode:     UNSUPPORTED_VERSION,
	}

	writeBytes(responseBody.CorrelationId, buff)
	writeBytes(responseBody.ErrorCode, buff)

	sentBuffer := addLength(buff)
	conn.Write(sentBuffer)
	os.Exit(0)

}

func addLength(buffer *bytes.Buffer) []byte {
	newBuffer := new(bytes.Buffer)
	length := int32(len(buffer.Bytes()))

	writeBytes(length, newBuffer)
	res := append(newBuffer.Bytes(), buffer.Bytes()...)

	return res
}

func writeBytes[T int8 | int16 | int32](resField T, buff *bytes.Buffer) {
	err := binary.Write(buff, binary.BigEndian, resField)
	if err != nil {
		fmt.Println("Error while writing to buffer [", err.Error(), "]")
		os.Exit(1)
	}

}

func sendSuccessResponse(conn net.Conn, correlationId int32) {
	responseBody := constructResponse(correlationId)
	buff := new(bytes.Buffer)

	responseBody.Encode(buff)

	sentBuffer := addLength(buff)
	_, err := conn.Write(sentBuffer)

	if err != nil {
		fmt.Println("Could not send the correlation id [", err.Error(), "]")
		os.Exit(1)
	}
}

func sendFetchResponse(conn net.Conn, correlationId int32) {
	fetchResponse := constructFetchResponse(correlationId)
	buff := new(bytes.Buffer)

	fetchResponse.Encode(buff)

	sentBuffer := addLength(buff)
	_, err := conn.Write(sentBuffer)

	if err != nil {
		fmt.Println("Could not send the fetch response [", err.Error(), "]")
		os.Exit(1)
	}
}

func handleRequest(conn net.Conn, db DataBody) {
	if db.RequestApiVersion < MIN_VERSION || db.RequestApiVersion > MAX_VERSION {
		sendErrorResponse(conn, db.CorrelationId)
		return
	}

	if db.RequestApiVersion == FETCH_REQUEST_VERSION {
		sendFetchResponse(conn, db.CorrelationId)
	} else {
		sendSuccessResponse(conn, db.CorrelationId)
	}

}

func handleConnection(conn net.Conn) {
	for {
		db, err := parseData(conn)
		if err != nil {
			fmt.Println("Could not read connection")
			return
		}

		handleRequest(conn, db)
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
		go handleConnection(conn)
	}

}
