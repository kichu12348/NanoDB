package vector

import "math"

func Dist(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return math.MaxFloat32
	}

	var sum float32
	for i := range v1 {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}

	return float32(math.Sqrt(float64(sum)))
}
