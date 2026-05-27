package json

import (
	"fmt"
	"strconv"
	"strings"
)

func writeValue(v Value) string {
	var b strings.Builder
	appendValue(&b, v)
	return b.String()
}

func appendValue(b *strings.Builder, v Value) {
	switch t := v.(type) {
	case StringValue:
		appendString(b, t.V)
	case NumberValue:
		b.WriteString(strconv.FormatFloat(t.V, 'g', -1, 64))
	case IntValue:
		b.WriteString(strconv.FormatInt(t.V, 10))
	case BoolValue:
		if t.V {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case NullValue:
		b.WriteString("null")
	case ArrayValue:
		b.WriteByte('[')
		for i, e := range t.Elements {
			if i > 0 {
				b.WriteByte(',')
			}
			appendValue(b, e)
		}
		b.WriteByte(']')
	case ObjectValue:
		b.WriteByte('{')
		for i, k := range t.Keys {
			if i > 0 {
				b.WriteByte(',')
			}
			appendString(b, k)
			b.WriteByte(':')
			appendValue(b, t.Fields[k])
		}
		b.WriteByte('}')
	}
}

func appendString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, c := range s {
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				b.WriteString(fmt.Sprintf(`\u%04x`, c))
			} else {
				b.WriteRune(c)
			}
		}
	}
	b.WriteByte('"')
}
