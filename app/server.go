package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/utils"
)

type cacheEntry struct {
	value string
	exp   time.Time
}

type safeCache struct {
	sync.RWMutex
	stored map[string]cacheEntry
}

var (
	cache     safeCache
	config    map[string]string
	NULL_RESP = []byte("$-1\r\n")
)

func main() {
	readArgs(os.Args[1:])
	port, ok := config["port"]
	if !ok {
		port = "6379"
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Printf("failed to bind to port %s\n", port)
		os.Exit(1)
	}
	defer listener.Close()

	cache = safeCache{
		stored: make(map[string]cacheEntry),
	}

	fmt.Printf("started redis server on port %s\n", port)

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
	case "CONFIG":
		return handleCommandConfig(cmd[1:])
	case "INFO":
		return handleCommandInfo(cmd[1:])
	default:
		return nil, nil
	}
}

func handleCommandSet(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("error SET, was expecting more arguments")
	}

	var exp time.Time
	if len(cmd) >= 4 && cmd[3].DataType == utils.STRING {
		content, err := strconv.Atoi(cmd[3].Content.(string))
		if err == nil {
			exp = time.Now().Add(time.Millisecond * time.Duration(content))
		}
	}
	cache.RWMutex.Lock()
	defer cache.RWMutex.Unlock()

	cache.stored[cmd[0].Content.(string)] = cacheEntry{
		value: cmd[1].Content.(string),
		exp:   exp,
	}

	return utils.EncodeResp("OK", utils.SIMPLE_STRING)
}

func handleCommandGet(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 1 {
		return nil, errors.New("error GET, was expecting more arguments")
	}

	cache.RWMutex.RLock()
	defer cache.RWMutex.RUnlock()

	key := cmd[0].Content.(string)
	stored, ok := cache.stored[key]

	if !ok {
		return NULL_RESP, nil
	}

	if !stored.exp.IsZero() && time.Now().After(stored.exp) {
		delete(cache.stored, key)
		return NULL_RESP, nil
	}

	return utils.EncodeResp(stored.value, utils.STRING)
}

func handleCommandInfo(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) == 0 || cmd[0].Content != "replication" {
		return NULL_RESP, nil
	}

	return utils.EncodeResp("role:master", utils.STRING)
}

func handleCommandConfig(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 2 || cmd[0].Content != "GET" {
		return NULL_RESP, nil
	}

	entry, ok := config[cmd[1].Content.(string)]
	if !ok {
		return NULL_RESP, nil
	}

	return utils.EncodeResp([]utils.Resp{cmd[1], {Content: entry, DataType: utils.STRING}}, utils.ARRAY)
}

func readArgs(args []string) {
	config = map[string]string{}
	for i := 0; i+1 < len(args); i += 2 {
		config[args[i][2:]] = args[i+1]
	}
}
