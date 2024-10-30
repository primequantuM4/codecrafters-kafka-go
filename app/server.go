package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
)

// VERSION TYPES
const MIN_VERSION = int16(0)
const MAX_VERSION = int16(16)
const FETCH_REQUEST_VERSION = int16(16)

const TAG_BUFFER = int8(0)

type Header struct {
	Length            int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientIdLength    int16
	ClientId          *string //Nullable Field
}

func parseData(connection net.Conn) (Header, bytes.Reader, error) {
	var header Header

	buffer := make([]byte, 1024)
	n, err := connection.Read(buffer)
	if err != nil {
		return header, *bytes.NewReader([]byte{}), err
	}

	newBuffer := buffer[:n]
	fmt.Println("Byte array is: ", newBuffer)
	reader := bytes.NewReader(newBuffer)

	// Header
	binary.Read(reader, binary.BigEndian, &header.Length)
	binary.Read(reader, binary.BigEndian, &header.RequestApiKey)
	binary.Read(reader, binary.BigEndian, &header.RequestApiVersion)
	binary.Read(reader, binary.BigEndian, &header.CorrelationId)
	binary.Read(reader, binary.BigEndian, &header.ClientIdLength)

	if header.ClientIdLength != -1 {
		clientIdBytes := make([]byte, header.ClientIdLength)
		binary.Read(reader, binary.BigEndian, &clientIdBytes)

		clientIdString := string(clientIdBytes)
		header.ClientId = &clientIdString
		fmt.Println("ClientId:", *header.ClientId)
	}

	binary.Read(reader, binary.BigEndian, TAG_BUFFER)
	fmt.Println("Length is: ", header.Length)
	fmt.Println("Cut length: ", n)
	fmt.Println("Current version is: ", header.RequestApiVersion)
	fmt.Printf("Header is: %+v\n", header)
	return header, *reader, nil
}

func prependLength(buffer *bytes.Buffer) []byte {
	newBuffer := new(bytes.Buffer)
	length := int32(len(buffer.Bytes()))

	writeBytes(length, newBuffer)
	res := append(newBuffer.Bytes(), buffer.Bytes()...)

	return res
}

func writeBytes[T int8 | int16 | int32 | int64 | []byte | uuid.UUID](resField T, buff *bytes.Buffer) {
	err := binary.Write(buff, binary.BigEndian, resField)
	if err != nil {
		fmt.Println("Error while writing to buffer [", err.Error(), "]")
		os.Exit(1)
	}

}

func handleRequest(conn net.Conn, hdr Header, reader *bytes.Reader) {
	if hdr.RequestApiVersion < MIN_VERSION || hdr.RequestApiVersion > MAX_VERSION {
		sendErrorResponse(conn, hdr.CorrelationId)
		return
	}

	if hdr.RequestApiVersion == FETCH_REQUEST_VERSION {
		// parse fetch body further
		fetchRequest := ParseRequest(reader)
		sendFetchResponse(conn, hdr.CorrelationId, fetchRequest)
	} else {
		sendSuccessResponse(conn, hdr.CorrelationId)
	}

}

func handleConnection(conn net.Conn) {
	for {
		hdr, reader, err := parseData(conn)
		if err != nil {
			fmt.Println("Could not read connection")
			return
		}

		handleRequest(conn, hdr, &reader)
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
