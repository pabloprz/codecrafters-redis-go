package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/utils"
)

type nodeRole string

const (
	MASTER       nodeRole = "master"
	SLAVE        nodeRole = "slave"
	EMPTY_RDB             = `524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2`
	ENTRY_STRING          = iota
	ENTRY_STREAM
)

type nodeInfo struct {
	id         string
	offset     int
	port       string
	role       nodeRole
	masterHost string
	masterConn net.Conn
	replicas   []net.Conn
}

type cacheEntryType int

func (t cacheEntryType) String() string {
	switch t {
	case ENTRY_STRING:
		return "string"
	case ENTRY_STREAM:
		return "stream"
	default:
		return ""
	}
}

type cacheEntry struct {
	value     any
	exp       time.Time
	entryType cacheEntryType
}

type safeCache struct {
	sync.RWMutex
	stored map[string]cacheEntry
}

func (c *safeCache) getKey(key string) (cacheEntry, bool) {
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()

	entry, ok := c.stored[key]
	return entry, ok
}

func (c *safeCache) setKey(key string, val any, exp time.Time, entryType cacheEntryType) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	entry := cacheEntry{
		value:     val,
		exp:       exp,
		entryType: entryType,
	}

	c.stored[key] = entry
}

func (c *safeCache) deleteKey(key string) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	delete(c.stored, key)
}

type streamId struct {
	msTime         int
	sequenceNumber int
}

func streamIdFromString(id string) streamId {
	splitted := strings.Split(id, "-")
	if len(splitted) < 2 {
		return streamId{0, 0}
	}

	ms, _ := strconv.Atoi(splitted[0])
	seq, _ := strconv.Atoi(splitted[1])
	return streamId{
		ms,
		seq,
	}
}

func (id streamId) String() string {
	return fmt.Sprintf("%d-%d", id.msTime, id.sequenceNumber)
}

type streamEntry struct {
	id streamId
}

type Stream struct {
	entries []streamEntry
}

func (s Stream) append(input string) error {
	id := streamIdFromString(input)

	latest := s.top()
	if latest == nil || latest.id.msTime > id.msTime || (latest.id.msTime == id.msTime && latest.id.sequenceNumber < id.sequenceNumber) {
		s.entries = append(s.entries, streamEntry{id})
		return nil
	}

	return errors.New("invalid entry")
}

func (s Stream) top() *streamEntry {
	if len(s.entries) == 0 {
		return nil
	}

	return &s.entries[len(s.entries)-1]
}

var (
	node      nodeInfo
	cache     safeCache
	config    map[string]string
	NULL_RESP = []byte("$-1\r\n")
)

func main() {
	initializeServer(os.Args[1:])

	listener, err := net.Listen("tcp", "0.0.0.0:"+node.port)
	if err != nil {
		fmt.Printf("failed to bind to port %s\n", node.port)
		os.Exit(1)
	}
	defer listener.Close()

	cache = safeCache{
		stored: make(map[string]cacheEntry),
	}

	fmt.Printf("started redis server on port %s\n", node.port)

	if node.role == SLAVE {
		go connectToMaster()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleClientConn(conn, false)
	}
}

func initializeServer(args []string) {
	config = map[string]string{}
	node = nodeInfo{}
	for i := 0; i+1 < len(args); i += 2 {
		switch flag := args[i][2:]; flag {
		case "port":
			node.port = args[i+1]
		case "replicaof":
			host := strings.SplitN(args[i+1], " ", 2)
			node.masterHost = strings.Join(host, ":")
		default:
			config[args[i][2:]] = args[i+1]
		}
	}

	if node.port == "" {
		node.port = "6379"
	}

	if node.masterHost == "" {
		node.role = MASTER
		node.id = generateRandomId()
	} else {
		node.role = SLAVE
	}
}

func connectToMaster() {
	conn, err := net.Dial("tcp", node.masterHost)
	if err != nil {
		fmt.Println("error connecting to master node, ", err)
		os.Exit(1)
	}

	node.masterConn = conn

	// Step 1 PING
	encodedPing := encodeCmd([]utils.Resp{{Content: "PING", DataType: utils.STRING}})
	conn.Write(encodedPing)

	response := make([]byte, 1024)
	conn.Read(response)

	// Step 2 REPLCONF
	encodedPort := encodeCmd([]utils.Resp{
		{Content: "REPLCONF", DataType: utils.STRING},
		{Content: "listening-port", DataType: utils.STRING},
		{Content: node.port, DataType: utils.STRING},
	})
	conn.Write(encodedPort)
	conn.Read(response)

	encodedCapa := encodeCmd([]utils.Resp{
		{Content: "REPLCONF", DataType: utils.STRING},
		{Content: "capa", DataType: utils.STRING},
		{Content: "psync2", DataType: utils.STRING},
	})
	conn.Write(encodedCapa)
	conn.Read(response)

	encodedSync := encodeCmd([]utils.Resp{
		{Content: "PSYNC", DataType: utils.STRING},
		{Content: "?", DataType: utils.STRING},
		{Content: "-1", DataType: utils.STRING},
	})
	conn.Write(encodedSync)
	handleClientConn(conn, true)
}

func handleClientConn(conn net.Conn, fromMaster bool) {
	defer conn.Close()

	fmt.Printf("new connection from %s\n", conn.RemoteAddr().String())

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Client connection closed", conn.RemoteAddr())
				return
			}
			fmt.Println("Error reading connection: ", err.Error())
			return
		}

		for nParsed := 0; nParsed < n; {
			parsed, offset, err := utils.ParseResp(buffer[nParsed:n])
			nParsed += offset - 1
			if err != nil {
				// TOOD write error
				fmt.Printf("Error parsing input from client %s\n", err)
				break
			}

			out, err := handleCommand(&parsed, conn)
			if err != nil {
				fmt.Println("Error handling command", err)
				continue
			}

			if !fromMaster || replicaMustRespond(&parsed) {
				conn.Write(out)
			}

			if node.role == SLAVE {
				node.offset += offset - 1
			}
		}
	}
}

func replicaMustRespond(input *utils.Resp) bool {
	if input.DataType != utils.ARRAY {
		return false
	}

	cmd := input.Content.([]utils.Resp)
	return cmd[0].Content == "REPLCONF" && cmd[1].Content == "GETACK"
}

func handleCommand(input *utils.Resp, conn net.Conn) ([]byte, error) {
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
	case "REPLCONF":
		return handleCommandReplConfig(cmd[1:])
	case "PSYNC":
		return handleCommandSync(cmd[1:], conn)
	case "WAIT":
		return handleCommandWait(cmd[1:])
	case "TYPE":
		return handleCommandType(cmd[1:])
	case "XADD":
		return handleCommandStreamAdd(cmd[1:])
	default:
		return nil, nil
	}
}

func handleCommandStreamAdd(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 2 {
		return nil, errors.New("error SET, was expecting more arguments")
	}

	key := cmd[0].Content.(string)
	id := cmd[1].Content.(string)

	stream, ok := cache.getKey(key)
	if !ok {
		stream = cacheEntry{
			entryType: ENTRY_STREAM,
			value:     Stream{make([]streamEntry, 0, 1)},
		}
		cache.setKey(key, stream, time.Time{}, ENTRY_STREAM)
	}

	stream.value.(Stream).append(id)
	return utils.EncodeResp(id, utils.STRING)
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
	cache.setKey(cmd[0].Content.(string), cmd[1].Content.(string), exp, ENTRY_STRING)

	if node.role == MASTER {
		bcast, err := utils.EncodeResp(
			append([]utils.Resp{{
				Content: "SET", DataType: utils.STRING,
			}}, cmd...), utils.ARRAY)
		if err != nil {
			return nil, err
		}
		for _, replica := range node.replicas {
			replica.Write(bcast)
		}
	}

	return utils.EncodeResp("OK", utils.SIMPLE_STRING)
}

func handleCommandGet(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) < 1 {
		return nil, errors.New("error GET, was expecting more arguments")
	}

	key := cmd[0].Content.(string)
	stored, ok := cache.getKey(key)

	if !ok {
		return NULL_RESP, nil
	}

	if !stored.exp.IsZero() && time.Now().After(stored.exp) {
		cache.deleteKey(key)
		return NULL_RESP, nil
	}

	return utils.EncodeResp(stored.value, utils.STRING)
}

func handleCommandWait(cmd []utils.Resp) ([]byte, error) {
	return utils.EncodeResp(len(node.replicas), utils.INTEGER)
}

func handleCommandInfo(cmd []utils.Resp) ([]byte, error) {
	if len(cmd) == 0 || cmd[0].Content != "replication" {
		return NULL_RESP, nil
	}

	resp := fmt.Sprintf("role:%s\n", node.role)

	if node.role == MASTER {
		resp = fmt.Sprintf("%smaster_replid:%s\nmaster_repl_offset:%d\n", resp, node.id, node.offset)
	}

	return utils.EncodeResp(resp, utils.STRING)
}

func handleCommandReplConfig(cmd []utils.Resp) ([]byte, error) {
	subCmd := strings.ToLower(cmd[0].Content.(string))
	if subCmd == "listening-port" || subCmd == "capa" {
		return utils.EncodeResp("OK", utils.SIMPLE_STRING)
	}

	if subCmd == "getack" {
		return utils.EncodeResp([]utils.Resp{
			{Content: "REPLCONF", DataType: utils.STRING},
			{Content: "ACK", DataType: utils.STRING},
			{Content: strconv.Itoa(node.offset), DataType: utils.STRING},
		}, utils.ARRAY)
	}

	return nil, nil
}

func handleCommandSync(cmd []utils.Resp, conn net.Conn) ([]byte, error) {
	resync, err := utils.EncodeResp(
		fmt.Sprintf("FULLRESYNC %s %d", node.id, node.offset),
		utils.SIMPLE_STRING,
	)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(resync)
	if err != nil {
		return nil, err
	}

	decoded, err := hex.DecodeString(EMPTY_RDB)
	if err != nil {
		return nil, err
	}

	node.replicas = append(node.replicas, conn)
	return utils.EncodeRdb(decoded), nil
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

func handleCommandType(cmd []utils.Resp) ([]byte, error) {
	key := cmd[0].Content.(string)

	val, ok := cache.getKey(key)

	if !ok {
		return utils.EncodeResp("none", utils.SIMPLE_STRING)
	}

	return utils.EncodeResp(val.entryType.String(), utils.STRING)
}

func generateRandomId() string {
	runes := []rune("0123456789")
	b := make([]rune, 40)
	for i := range 40 {
		b[i] = runes[rand.IntN(len(runes))]
	}
	return string(b)
}

func encodeCmd(cmd []utils.Resp) []byte {
	encodedPing, err := utils.EncodeResp(cmd, utils.ARRAY)
	if err != nil {
		fmt.Println("error encoding ping, ", err)
		os.Exit(1)
	}
	return encodedPing
}
