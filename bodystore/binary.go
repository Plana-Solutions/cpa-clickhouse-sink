package bodystore

import (
	"bytes"
	"encoding/binary"
	"time"
)

const SkeletonStructure = "lineage UInt64, body_len UInt32, body_sha256 FixedString(32), body_residual String, body_ref_offsets Array(UInt32), body_refs Array(FixedString(16))"

type binaryWriter struct{ bytes.Buffer }

func (w *binaryWriter) n(n int) {
	var b [10]byte
	k := binary.PutUvarint(b[:], uint64(n))
	w.Write(b[:k])
}
func (w *binaryWriter) u(n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	w.Write(b[:])
}
func (w *binaryWriter) u32(n uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], n)
	w.Write(b[:])
}
func EncodeSkeleton(lineage uint64, sk Skeleton) []byte {
	w := new(binaryWriter)
	w.u(lineage)
	w.u32(sk.Length)
	w.Write(sk.SHA256[:])
	w.n(len(sk.Residual))
	w.Write(sk.Residual)
	w.n(len(sk.Offsets))
	for _, o := range sk.Offsets {
		w.u32(o)
	}
	w.n(len(sk.Refs))
	for _, h := range sk.Refs {
		w.Write(h[:])
	}
	return w.Bytes()
}
func EncodeBlob(lineage uint64, h Hash, data []byte, firstSeen time.Time) []byte {
	w := new(binaryWriter)
	w.u(lineage)
	w.Write(h[:])
	w.n(len(data))
	w.Write(data)
	w.u(uint64(firstSeen.UnixNano()))
	return w.Bytes()
}
