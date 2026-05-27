package schema

// PhysicalKind is a wire tag (Layer 0 spec §4.1).
type PhysicalKind byte

const (
	PhysicalNull PhysicalKind = 0x00
	PhysicalBool PhysicalKind = 0x01
	PhysicalInt8 PhysicalKind = 0x02
	PhysicalInt16 PhysicalKind = 0x03
	PhysicalInt32 PhysicalKind = 0x04
	PhysicalInt64 PhysicalKind = 0x05
	PhysicalFloat32 PhysicalKind = 0x06
	PhysicalFloat64 PhysicalKind = 0x07
	PhysicalBytes PhysicalKind = 0x08
	PhysicalString PhysicalKind = 0x09
	PhysicalArray PhysicalKind = 0x0A
	PhysicalMap PhysicalKind = 0x0B
	PhysicalRecord PhysicalKind = 0x0C
	PhysicalEnum PhysicalKind = 0x0D
	PhysicalUnion PhysicalKind = 0x0E
	PhysicalFixed PhysicalKind = 0x0F
)

var byTag = map[byte]PhysicalKind{
	0x00: PhysicalNull, 0x01: PhysicalBool, 0x02: PhysicalInt8, 0x03: PhysicalInt16,
	0x04: PhysicalInt32, 0x05: PhysicalInt64, 0x06: PhysicalFloat32, 0x07: PhysicalFloat64,
	0x08: PhysicalBytes, 0x09: PhysicalString, 0x0A: PhysicalArray, 0x0B: PhysicalMap,
	0x0C: PhysicalRecord, 0x0D: PhysicalEnum, 0x0E: PhysicalUnion, 0x0F: PhysicalFixed,
}

func PhysicalFromTag(tag byte) (PhysicalKind, bool) {
	k, ok := byTag[tag]
	return k, ok
}

func (p PhysicalKind) Tag() byte { return byte(p) }
