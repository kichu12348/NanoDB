package vector

import (
	"encoding/binary"
	"math"
)

func VectorToBytes(v []float32) []byte {
	buff := make([]byte, len(v)*4)

	for i, f := range v {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(buff[i*4:], bits)
	}

	return buff
}

func VectorFromBytes(data []byte) []float32 {
	numFloats := len(data) / 4
	v := make([]float32, numFloats)

	for i := range numFloats {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		v[i] = math.Float32frombits(bits)
	}
	return v
}

func InitVectorPage(data []byte) {
	binary.LittleEndian.PutUint32(data[0:4], 0)
	binary.LittleEndian.PutUint16(data[4:6], 0)
}
