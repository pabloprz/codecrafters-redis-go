package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/utils"
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

		go handleClientConn(conn)
	}
}

func handleClientConn(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("new connection from %s\n", conn.RemoteAddr().String())

	buffer := make([]byte, 1028)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Client connection closed")
				return
			}
			fmt.Println("Error reading connection: ", err.Error())
			return
		}

		parsed, _, err := utils.ParseResp(buffer[:n])
		if err != nil {
			// TOOD write error
			fmt.Printf("Error parsing input from client %s\n", err)
			return
		}

		out, err := handleCommand(&parsed)
		if err != nil {
			fmt.Println("Error handling command", err)
		}
		conn.Write(out)
	}
}

func handleCommand(resp *utils.Resp) ([]byte, error) {
	if resp.DataType != utils.ARRAY {
		return nil, errors.New("invalid client input, was expecting array")
	}

	cmd := resp.Content.([]utils.Resp)
	switch cmd[0].Content {
	case "PING":
		return utils.EncodeResp("PONG", utils.SIMPLE_STRING)
		return []byte("+PONG\r\n"), nil
	case "ECHO":
		return utils.EncodeResp(cmd[1].Content.(string), utils.STRING)
	default:
		return nil, nil
	}
}
