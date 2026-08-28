package codec

import (
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec/schema"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

func wireEncode(v Value, typ schema.Type, reg *schema.Registry) ([]byte, error) {
	var buf []byte
	if err := encodeValue(&buf, v, typ, reg); err != nil {
		return nil, err
	}
	return buf, nil
}

func wireDecode(bytes []byte, typ schema.Type, reg *schema.Registry) (Value, error) {
	v, consumed, err := wireDecodeFirst(bytes, typ, reg)
	if err != nil {
		return nil, err
	}
	if consumed != len(bytes) {
		return nil, kdberr.NewDecodeError("trailing bytes", consumed, nil)
	}
	return v, nil
}

// wireDecodeFirst decodes one value and returns bytes consumed (may leave a suffix).
func wireDecodeFirst(bytes []byte, typ schema.Type, reg *schema.Registry) (Value, int, error) {
	c := &cursor{raw: bytes, limit: len(bytes)}
	v, err := decodeValue(c, typ, reg)
	if err != nil {
		return nil, 0, err
	}
	return v, c.pos, nil
}

func encodeValue(buf *[]byte, v Value, typ schema.Type, reg *schema.Registry) error {
	switch t := typ.(type) {
	case schema.Nullable:
		if _, ok := v.(NullValue); ok {
			*buf = append(*buf, 0)
			return nil
		}
		*buf = append(*buf, 1)
		return encodeValue(buf, v, t.Inner, reg)
	case schema.Union:
		uv, ok := v.(UnionValue)
		if !ok {
			return kdberr.NewEncodeError("UnionVal expected", nil)
		}
		if uv.Branch < 0 || uv.Branch >= len(t.Branches) {
			return kdberr.NewEncodeError("branch", nil)
		}
		*buf = append(*buf, byte(uv.Branch))
		return encodeValue(buf, uv.Inner, t.Branches[uv.Branch], reg)
	case schema.Primitive:
		return encodePrim(buf, v, t.Physical, t.Logical)
	case schema.Array:
		a, ok := v.(ArrayValue)
		if !ok {
			return kdberr.NewEncodeError("ArrayVal expected", nil)
		}
		*buf = appendLeb128U32(*buf, uint32(len(a.Elements)))
		for _, e := range a.Elements {
			if err := encodeValue(buf, e, t.Element, reg); err != nil {
				return err
			}
		}
		return nil
	case schema.Map:
		m, ok := v.(MapValue)
		if !ok {
			return kdberr.NewEncodeError("MapVal expected", nil)
		}
		*buf = appendLeb128U32(*buf, uint32(len(m.Entries)))
		for _, e := range m.Entries {
			if err := encodeValue(buf, e.Key, t.Key, reg); err != nil {
				return err
			}
			if err := encodeValue(buf, e.Val, t.Value, reg); err != nil {
				return err
			}
		}
		return nil
	case schema.Ref:
		return encodeNamed(buf, v, t, reg)
	default:
		return kdberr.NewEncodeError("unknown type", nil)
	}
}

func encodeNamed(buf *[]byte, v Value, ref schema.Ref, reg *schema.Registry) error {
	named, err := reg.Resolve(ref.FullyQualifiedName)
	if err != nil {
		return err
	}
	switch s := named.(type) {
	case *schema.RecordSchema:
		rec, ok := v.(RecordValue)
		if !ok {
			return kdberr.NewEncodeError("RecordVal expected", nil)
		}
		return encodeRecord(buf, rec, s, reg)
	case *schema.EnumSchema:
		e, ok := v.(EnumValue)
		if !ok {
			return kdberr.NewEncodeError("EnumVal expected", nil)
		}
		if e.Ordinal < 0 || e.Ordinal >= len(s.Symbols) {
			return kdberr.NewEncodeError("enum ordinal range", nil)
		}
		*buf = append(*buf, putLe32s(int32(e.Ordinal))...)
		return nil
	case *schema.FixedSchema:
		f, ok := v.(FixedValue)
		if !ok {
			return kdberr.NewEncodeError("FixedVal expected", nil)
		}
		if len(f.V) != s.Size {
			return kdberr.NewEncodeError("fixed bytes", nil)
		}
		*buf = append(*buf, f.V...)
		return nil
	default:
		return kdberr.NewEncodeError("unsupported named type", nil)
	}
}

func encodeRecord(buf *[]byte, rec RecordValue, sch *schema.RecordSchema, reg *schema.Registry) error {
	var body []byte
	// Field order must be by id, but a schema's Fields are declared in id order
	// in practice, so check before copying: sorting a copy on every single
	// record encode cost an allocation plus an O(n log n) sort to reproduce the
	// order the slice was already in. sch.Fields is frozen (see Registry.Freeze),
	// so it is never sorted in place here.
	fields := sch.Fields
	byID := func(i, j int) bool { return fields[i].ID < fields[j].ID }
	if !sort.SliceIsSorted(fields, byID) {
		fields = make([]schema.FieldSchema, len(sch.Fields))
		copy(fields, sch.Fields)
		sort.Slice(fields, func(i, j int) bool { return fields[i].ID < fields[j].ID })
	}
	for _, f := range fields {
		cur, ok := rec.Fields[f.ID]
		if !ok {
			if f.Default != nil {
				continue
			}
			return kdberr.NewEncodeError("missing field "+f.Name, nil)
		}
		if omitField(cur, f.Default) {
			continue
		}
		body = appendLeb128U32(body, uint32(f.ID))
		body = append(body, physicalTag(f.Type, reg).Tag())
		if err := encodeValue(&body, cur, f.Type, reg); err != nil {
			return err
		}
	}
	*buf = appendLeb128U32(*buf, uint32(len(body)))
	*buf = append(*buf, body...)
	return nil
}

func omitField(cur Value, def any) bool {
	if def == nil {
		return false
	}
	// shallow equality for primitives used as defaults
	return valuesEqual(cur, def)
}

func valuesEqual(a Value, b any) bool {
	switch av := a.(type) {
	case NullValue:
		_, ok := b.(NullValue)
		return ok
	case Int32Value:
		bv, ok := b.(Int32Value)
		return ok && av.V == bv.V
	case StringValue:
		bv, ok := b.(StringValue)
		return ok && av.V == bv.V
	case BoolValue:
		bv, ok := b.(BoolValue)
		return ok && av.V == bv.V
	default:
		return false
	}
}

func physicalTag(typ schema.Type, reg *schema.Registry) schema.PhysicalKind {
	switch t := typ.(type) {
	case schema.Primitive:
		return t.Physical
	case schema.Array:
		return schema.PhysicalArray
	case schema.Map:
		return schema.PhysicalMap
	case schema.Union, schema.Nullable:
		return schema.PhysicalUnion
	case schema.Ref:
		named, err := reg.Resolve(t.FullyQualifiedName)
		if err != nil {
			return schema.PhysicalRecord
		}
		switch named.(type) {
		case *schema.RecordSchema:
			return schema.PhysicalRecord
		case *schema.EnumSchema:
			return schema.PhysicalEnum
		case *schema.FixedSchema:
			return schema.PhysicalFixed
		}
	}
	return schema.PhysicalRecord
}

func encodePrim(buf *[]byte, v Value, phy schema.PhysicalKind, logical schema.LogicalAnnotation) error {
	switch logical.(type) {
	case schema.LogicalDate:
		d, ok := v.(DateValue)
		if !ok {
			if i, ok := v.(Int32Value); ok {
				*buf = append(*buf, putLe32s(i.V)...)
				return nil
			}
			return kdberr.NewEncodeError("DATE value", nil)
		}
		*buf = append(*buf, putLe32s(d.DaysSinceEpoch)...)
		return nil
	case schema.LogicalTimestampMicros:
		ts, ok := v.(TimestampValue)
		if !ok {
			return kdberr.NewEncodeError("timestamp", nil)
		}
		*buf = append(*buf, putLe64s(ts.EpochMicros)...)
		return nil
	case schema.LogicalUUID:
		*buf = append(*buf, uuidWire(v)...)
		return nil
	case nil:
		return encodePhysicalOnly(buf, v, phy)
	default:
		return encodePhysicalOnly(buf, v, phy)
	}
}

func encodePhysicalOnly(buf *[]byte, v Value, phy schema.PhysicalKind) error {
	switch phy {
	case schema.PhysicalNull:
		if _, ok := v.(NullValue); !ok {
			return kdberr.NewEncodeError("null", nil)
		}
	case schema.PhysicalBool:
		b := v.(BoolValue)
		if b.V {
			*buf = append(*buf, 1)
		} else {
			*buf = append(*buf, 0)
		}
	case schema.PhysicalInt32:
		*buf = append(*buf, putLe32s(v.(Int32Value).V)...)
	case schema.PhysicalInt64:
		*buf = append(*buf, putLe64s(v.(Int64Value).V)...)
	case schema.PhysicalFloat64:
		d := v.(Float64Value).V
		if math.IsInf(d, 0) || math.IsNaN(d) {
			return kdberr.NewEncodeError("non-finite", nil)
		}
		*buf = append(*buf, putFloat64(d)...)
	case schema.PhysicalString:
		*buf = appendLebString(*buf, v.(StringValue).V)
	case schema.PhysicalBytes:
		*buf = appendLebBytes(*buf, v.(BytesValue).V)
	default:
		return kdberr.NewEncodeError("composite needs structural type", nil)
	}
	return nil
}

func uuidWire(v Value) []byte {
	switch u := v.(type) {
	case UUIDValue:
		out := make([]byte, 16)
		writeBE64(u.MSB, out, 0)
		writeBE64(u.LSB, out, 8)
		return out
	case FixedValue:
		if len(u.V) != 16 {
			panic("uuid fixed size")
		}
		out := make([]byte, 16)
		copy(out, u.V)
		return out
	default:
		panic("uuid value")
	}
}

func decodeValue(c *cursor, typ schema.Type, reg *schema.Registry) (Value, error) {
	switch t := typ.(type) {
	case schema.Nullable:
		m, err := c.u8()
		if err != nil {
			return nil, err
		}
		switch m {
		case 0:
			return Null, nil
		case 1:
			return decodeValue(c, t.Inner, reg)
		default:
			return nil, kdberr.NewDecodeError("bad nullable", c.pos, nil)
		}
	case schema.Union:
		br, err := c.u8()
		if err != nil {
			return nil, err
		}
		if int(br) >= len(t.Branches) {
			return nil, kdberr.NewDecodeError("bad union", c.pos, nil)
		}
		inner, err := decodeValue(c, t.Branches[br], reg)
		if err != nil {
			return nil, err
		}
		return UnionValue{Branch: int(br), Inner: inner}, nil
	case schema.Primitive:
		return decodePrim(c, t.Physical, t.Logical)
	case schema.Array:
		n, err := c.leb()
		if err != nil {
			return nil, err
		}
		if err := c.checkElementCount(n, "array"); err != nil {
			return nil, err
		}
		list := make([]Value, 0, n)
		for i := uint64(0); i < n; i++ {
			el, err := decodeValue(c, t.Element, reg)
			if err != nil {
				return nil, err
			}
			list = append(list, el)
		}
		return ArrayValue{Elements: list}, nil
	case schema.Map:
		n, err := c.leb()
		if err != nil {
			return nil, err
		}
		if err := c.checkElementCount(n, "map"); err != nil {
			return nil, err
		}
		entries := make([]MapEntry, 0, n)
		for i := uint64(0); i < n; i++ {
			k, err := decodeValue(c, t.Key, reg)
			if err != nil {
				return nil, err
			}
			v, err := decodeValue(c, t.Value, reg)
			if err != nil {
				return nil, err
			}
			entries = append(entries, MapEntry{Key: k, Val: v})
		}
		return MapValue{Entries: entries}, nil
	case schema.Ref:
		return decodeNamed(c, t, reg)
	default:
		return nil, kdberr.NewDecodeError("unknown type", c.pos, nil)
	}
}

func decodeNamed(c *cursor, ref schema.Ref, reg *schema.Registry) (Value, error) {
	named, err := reg.Resolve(ref.FullyQualifiedName)
	if err != nil {
		return nil, err
	}
	switch s := named.(type) {
	case *schema.RecordSchema:
		return decodeRecord(c, s, reg)
	case *schema.EnumSchema:
		raw, err := c.rawN(4)
		if err != nil {
			return nil, err
		}
		ord := int(readLe32(raw, 0))
		sym := "<unknown>"
		if ord >= 0 && ord < len(s.Symbols) {
			sym = s.Symbols[ord]
		}
		return EnumValue{Ordinal: ord, Symbol: sym}, nil
	case *schema.FixedSchema:
		raw, err := c.rawN(s.Size)
		if err != nil {
			return nil, err
		}
		return upliftFixed(s, raw)
	default:
		return nil, kdberr.NewDecodeError("named", c.pos, nil)
	}
}

func decodeRecord(c *cursor, sch *schema.RecordSchema, reg *schema.Registry) (Value, error) {
	slabLen, err := c.leb()
	if err != nil {
		return nil, err
	}
	slab, err := c.rawN(int(slabLen))
	if err != nil {
		return nil, err
	}
	pos := 0
	fields := make(map[int]Value)
	for pos < len(slab) {
		fidU, err := readLeb128U64(slab, &pos)
		if err != nil {
			return nil, err
		}
		fid := int(fidU)
		if pos >= len(slab) {
			return nil, kdberr.NewDecodeError("truncated record", pos, nil)
		}
		tag := slab[pos]
		pos++
		kind, ok := schema.PhysicalFromTag(tag)
		if !ok {
			return nil, kdberr.NewDecodeError("unknown record tag", pos, nil)
		}
		field, ok := sch.FieldByID(fid)
		if !ok {
			// skip unknown field
			sub := &cursor{raw: slab, pos: pos, limit: len(slab)}
			if err := skipTagged(sub, kind); err != nil {
				return nil, err
			}
			pos = sub.pos
			continue
		}
		if kind != physicalTag(field.Type, reg) {
			return nil, kdberr.NewDecodeError("record tag mismatch", pos, nil)
		}
		sub := &cursor{raw: slab, pos: pos, limit: len(slab)}
		val, err := decodeValue(sub, field.Type, reg)
		if err != nil {
			return nil, err
		}
		pos = sub.pos
		fields[fid] = val
	}
	for _, f := range sch.Fields {
		if _, ok := fields[f.ID]; !ok && f.Default != nil {
			if dv, ok := f.Default.(Value); ok {
				fields[f.ID] = dv
			}
		}
	}
	return RecordValue{Fields: fields}, nil
}

func skipTagged(c *cursor, kind schema.PhysicalKind) error {
	switch kind {
	case schema.PhysicalInt32, schema.PhysicalFloat32:
		_, err := c.rawN(4)
		return err
	case schema.PhysicalInt64, schema.PhysicalFloat64:
		_, err := c.rawN(8)
		return err
	case schema.PhysicalString, schema.PhysicalBytes:
		_, err := readLebBytes(c)
		return err
	case schema.PhysicalBool, schema.PhysicalInt8:
		_, err := c.u8()
		return err
	case schema.PhysicalInt16:
		_, err := c.rawN(2)
		return err
	default:
		return kdberr.NewDecodeError("skip unsupported", c.pos, nil)
	}
}

func decodePrim(c *cursor, phy schema.PhysicalKind, logical schema.LogicalAnnotation) (Value, error) {
	switch logical.(type) {
	case schema.LogicalDate:
		raw, err := c.rawN(4)
		if err != nil {
			return nil, err
		}
		return DateValue{DaysSinceEpoch: int32(readLe32(raw, 0))}, nil
	case schema.LogicalTimestampMicros:
		raw, err := c.rawN(8)
		if err != nil {
			return nil, err
		}
		return TimestampValue{EpochMicros: int64(readLe64(raw, 0))}, nil
	case schema.LogicalUUID:
		raw, err := c.rawN(16)
		if err != nil {
			return nil, err
		}
		return UUIDValue{MSB: int64(readBE64(raw, 0)), LSB: int64(readBE64(raw, 8))}, nil
	case nil:
		return decodePhysicalOnly(c, phy)
	default:
		return decodePhysicalOnly(c, phy)
	}
}

func decodePhysicalOnly(c *cursor, phy schema.PhysicalKind) (Value, error) {
	switch phy {
	case schema.PhysicalNull:
		return Null, nil
	case schema.PhysicalBool:
		b, err := c.u8()
		if err != nil {
			return nil, err
		}
		return BoolValue{V: b != 0}, nil
	case schema.PhysicalInt32:
		raw, err := c.rawN(4)
		if err != nil {
			return nil, err
		}
		return Int32Value{V: int32(readLe32(raw, 0))}, nil
	case schema.PhysicalInt64:
		raw, err := c.rawN(8)
		if err != nil {
			return nil, err
		}
		return Int64Value{V: int64(readLe64(raw, 0))}, nil
	case schema.PhysicalFloat64:
		raw, err := c.rawN(8)
		if err != nil {
			return nil, err
		}
		return Float64Value{V: readFloat64(raw)}, nil
	case schema.PhysicalString:
		s, err := readLebString(c)
		if err != nil {
			return nil, err
		}
		return StringValue{V: s}, nil
	case schema.PhysicalBytes:
		b, err := readLebBytes(c)
		if err != nil {
			return nil, err
		}
		return BytesValue{V: b}, nil
	default:
		return nil, kdberr.NewDecodeError("unexpected composite physical", c.pos, nil)
	}
}

func upliftFixed(s *schema.FixedSchema, bytes []byte) (Value, error) {
	if len(bytes) != s.Size {
		return nil, kdberr.NewDecodeError("fixed size mismatch", -1, nil)
	}
	switch s.Logical.(type) {
	case schema.LogicalUUID:
		return UUIDValue{MSB: int64(readBE64(bytes, 0)), LSB: int64(readBE64(bytes, 8))}, nil
	default:
		out := make([]byte, len(bytes))
		copy(out, bytes)
		return FixedValue{V: out}, nil
	}
}
