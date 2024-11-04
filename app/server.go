package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/utils"
)

type safeCache struct {
	sync.RWMutex
	stored map[string]string
}

var (
	cache     safeCache
	NULL_RESP = []byte("$-1\r\n")
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()

	cache = safeCache{
		stored: make(map[string]string),
	}

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

func handleCommand(input *utils.Resp) ([]byte, error) {
	if input.DataType != utils.ARRAY {
		return nil, errors.New("invalid client input, was expecting array")
	}

	cmd := input.Content.([]utils.Resp)
	switch strings.ToUpper(cmd[0].Content.(string)) {
	case "PING":
		return utils.EncodeResp("PONG", utils.SIMPLE_STRING)
	case "ECHO":
		return utils.EncodeResp(cmd[1].Content.(string), utils.STRING)
	case "GET":
		return handleCommandGet(cmd[1:])
	case "SET":
		return handleCommandSet(cmd[1:])
	default:
		return nil, nil
	}
}

func handleCommandSet(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("error SET, was expecting more arguments")
	}

	cache.RWMutex.Lock()
	defer cache.RWMutex.Unlock()

	cache.stored[cmd[0].Content.(string)] = cmd[1].Content.(string)

	return utils.EncodeResp("OK", utils.SIMPLE_STRING)
}

func handleCommandGet(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 1 {
		return nil, errors.New("error GET, was expecting more arguments")
	}

	cache.RWMutex.RLock()
	defer cache.RWMutex.RUnlock()

	stored, ok := cache.stored[cmd[0].Content.(string)]

	if !ok {
		return NULL_RESP, nil
	}

	return utils.EncodeResp(stored, utils.STRING)
}
