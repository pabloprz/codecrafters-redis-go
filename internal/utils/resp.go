package utils

import (
	"bytes"
	"errors"
	"strconv"
	"unicode"
)

const (
	SIMPLE_STRING = '+'
	STRING        = '$'
	INTEGER       = ':'
	ARRAY         = '*'
)

type RespType byte

type Resp struct {
	Content  any
	DataType RespType
}

func ParseResp(buf []byte) (Resp, int, error) {
	resp := Resp{}
	if len(buf) == 0 {
		return resp, 0, errors.New("invalid resp")
	}

	switch buf[0] {
	case SIMPLE_STRING:
		return parseSimpleString(buf[1:])
	case STRING:
		return parseString(buf[1:])
	case INTEGER:
		return parseInteger(buf[1:])
	case ARRAY:
		return parseArray(buf[1:])
	default:
		return resp, 0, errors.ErrUnsupported
	}
}

func parseSimpleString(buf []byte) (Resp, int, error) {
	return Resp{}, 0, nil
}

// <length>\r\n<data>\r\n
func parseString(buf []byte) (Resp, int, error) {
	resp := Resp{DataType: STRING}
	i, length := 0, 0
	for i < len(buf) && unicode.IsDigit(rune(buf[i])) {
		length = length*10 + int(buf[i]-'0')
		i++
	}

	i += 2
	if i+length >= len(buf) || buf[i-2] != '\r' || buf[i-1] != '\n' {
		return resp, 0, errors.New("error parsing string. Invalid format")
	}

	resp.Content = string(buf[i : i+length])
	return resp, i + length + 2, nil
}

func parseInteger(buf []byte) (Resp, int, error) {
	return Resp{}, 0, nil
}

// <number-of-elements>\r\n<element-1>...<element-n>
func parseArray(buf []byte) (Resp, int, error) {
	resp := Resp{DataType: ARRAY}
	i, length := 0, 0
	for i < len(buf) && unicode.IsDigit(rune(buf[i])) {
		length = length*10 + int(buf[i]-'0')
		i++
	}

	i += 2
	if i >= len(buf) || buf[i-2] != '\r' || buf[i-1] != '\n' {
		return resp, 0, errors.New("error parsing array. Invalid format")
	}

	parsed := make([]Resp, 0, length)

	for length > 0 {
		element, n, err := ParseResp(buf[i:])
		if err != nil {
			return resp, 0, err
		}
		parsed = append(parsed, element)
		i += n + 1 // TODO LENGTH FROM PARSERESP
		length--
	}

	resp.Content = parsed
	return resp, i + 2, nil
}

func EncodeResp(val any, valType RespType) ([]byte, error) {
	switch valType {
	case SIMPLE_STRING:
		return encodeSimpleString(val.(string))
	case STRING:
		return encodeString(val.(string))
	default:
		return nil, nil
	}
}

func encodeString(val string) ([]byte, error) {
	var res bytes.Buffer
	res.WriteByte(STRING)
	res.WriteString(strconv.Itoa(len(val)))
	res.WriteByte('\r')
	res.WriteByte('\n')
	res.WriteString(val)
	res.WriteByte('\r')
	res.WriteByte('\n')

	return res.Bytes(), nil
}

func encodeSimpleString(val string) ([]byte, error) {
	return []byte(string(SIMPLE_STRING) + val + "\r\n"), nil
}
