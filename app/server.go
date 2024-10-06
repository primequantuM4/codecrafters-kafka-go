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

	length := int32(6)
	db, err := parseData(conn)

	errorCode := int16(35)

	if db.requestApiVersion > 4 || db.requestApiVersion < 0 {
		errorCode = 35
	} else {
		errorCode = 35
	}

	buff := new(bytes.Buffer)

	if err != nil {
		fmt.Println("Could not parse data properly")
		os.Exit(1)
	}

	fmt.Println("Recieved correlation id of: ", db.correlationId)

	err = binary.Write(buff, binary.BigEndian, length)
	if err != nil {
		fmt.Println("Error writing length")
		os.Exit(1)
	}

	err = binary.Write(buff, binary.BigEndian, db.correlationId)
	if err != nil {
		fmt.Println("Could not convert correlation id to byte array")
		os.Exit(1)
	}

	err = binary.Write(buff, binary.BigEndian, errorCode)
	if err != nil {
		fmt.Println("Could not validate the error code type")
		os.Exit(1)
	}
	_, err = conn.Write(buff.Bytes())

	if err != nil {
		fmt.Println("Could not send the correlation id")
		os.Exit(1)
	}
}
