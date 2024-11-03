package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("started redis server on port 6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		handleClientConn(conn)
	}
}

func handleClientConn(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("new connection from %s\n", conn.LocalAddr().String())

	buffer := make([]byte, 1028)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading connection: ", err.Error())
			return
		}

		fmt.Printf("Received %d bytes %s\n", n, buffer[:n])

		conn.Write([]byte("+PONG\r\n"))
	}
}
