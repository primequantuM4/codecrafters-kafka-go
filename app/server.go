package main

import (
	"fmt"
	"net"
	"os"
)

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

	buf := make([]byte, 40)
	n, err := conn.Read(buf)

	if err != nil {
		fmt.Println("Could not recieve data from broker")
		os.Exit(1)
	}

	fmt.Println("Recieved message length of: ", n)

	_, err = conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 7})

	if err != nil {
		fmt.Println("Could not send the correlation id")
		os.Exit(1)
	}
}
