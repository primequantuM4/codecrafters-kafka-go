package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func parseData(connection net.Conn) (int32, error) {
	var length int16
	var requestApiVersion int16
	var correlationId int32

	buffer := make([]byte, 1024)
	n, err := connection.Read(buffer)
	if err != nil {
		return -1, err
	}

	newBuffer := buffer[:n]
	fmt.Println("Byte array is: ", newBuffer)
	reader := bytes.NewReader(newBuffer)

	binary.Read(reader, binary.BigEndian, &length)
	binary.Read(reader, binary.BigEndian, &requestApiVersion)
	binary.Read(reader, binary.BigEndian, &correlationId)

	return correlationId, nil
}
func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	} else {
		fmt.Println("Listening on port", l.Addr().String())
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	length := int32(4)
	corrId, err := parseData(conn)

	buff := new(bytes.Buffer)

	if err != nil {
		fmt.Println("Could not parse data properly")
		os.Exit(1)
	}

	fmt.Println("Recieved correlation id of: ", corrId)

	err = binary.Write(buff, binary.BigEndian, length)
	if err != nil {
		fmt.Println("Error writing length")
		os.Exit(1)
	}

	err = binary.Write(buff, binary.BigEndian, corrId)
	if err != nil {
		fmt.Println("Could not convert correlation id to byte array")
		os.Exit(1)
	}

	_, err = conn.Write(buff.Bytes())

	if err != nil {
		fmt.Println("Could not send the correlation id")
		os.Exit(1)
	}
}
