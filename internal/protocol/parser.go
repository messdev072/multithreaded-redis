package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

func ParseRESP(r *bufio.Reader) (RESPType, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // Simple String
		line, _ := r.ReadString('\n')
		return SimpleString(trim(line)), nil
	case '-': // Error
		line, _ := r.ReadString('\n')
		return Error(trim(line)), nil
	case ':': // Integer
		line, _ := r.ReadString('\n')
		val, _ := strconv.ParseInt(trim(line), 10, 64)
		return Integer(val), nil
	case '$': // Bulk String
		line, _ := r.ReadString('\n')
		length, _ := strconv.Atoi(trim(line))
		if length == -1 {
			return BulkString(nil), nil
		}
		buf := make([]byte, length+2) // +2 for \r\n
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}
		return BulkString(buf[:length]), nil
	case '*': // Array
		line, _ := r.ReadString('\n')
		length, _ := strconv.Atoi(trim(line))
		if length == -1 {
			return Array(nil), nil
		}
		arr := make(Array, length)
		for i := 0; i < length; i++ {
			elem, err := ParseRESP(r)
			if err != nil {
				return nil, err
			}
			arr[i] = elem
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("invalid RESP prefix: %q", prefix)
	}
}

func trim(s string) string {
	if len(s) == 0 {
		return s
	}
	if s[len(s)-2:] == "\r\n" {
		s = s[:len(s)-2]
	}
	if s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	return s
}
