package protocol

import (
	"fmt"
	"strings"
)

type RESPType interface{}

type SimpleString string
type Error string
type Integer int64
type BulkString []byte
type Array []RESPType

// Encode helpers
func Encode(v RESPType) string {
	switch x := v.(type) {
	case SimpleString:
		return fmt.Sprintf("+%s\r\n", string(x))
	case Error:
		return fmt.Sprintf("-%s\r\n", string(x))
	case Integer:
		return fmt.Sprintf(":%d\r\n", int64(x))
	case BulkString:
		if x == nil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(x), x)
	case Array:
		if x == nil {
			return "*-1\r\n"
		}
		var b strings.Builder
		b.WriteString(fmt.Sprintf("*%d\r\n", len(x)))
		for _, elem := range x {
			b.WriteString(Encode(elem))
		}
		return b.String()
	default:
		return "-ERR unknown type\r\n"
	}
}
