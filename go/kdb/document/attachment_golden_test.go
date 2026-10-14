package document

// attachmentRefGolden pins AttachmentRef.Hash for TestAttachmentRefHashIsStable: SHA-256 of
// 'A' | 32 x 0xab | be64(1234567) | be32(9) | "image/png", computed independently.
const attachmentRefGolden = "56bceea2e09e70d29a834bd3190e8211a40b2a4eb77c931439ce22bb6c0658d5"
