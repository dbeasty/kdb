package vector

import (
	"fmt"
	"math"
	"strings"
)

// Metric is a similarity: higher is always better (§7).
type Metric int

const (
	// Cosine: dot / (‖a‖‖b‖), 0 when either norm is 0.
	Cosine Metric = iota
	// L2: 1 / (1 + ‖a − b‖).
	L2
	// InnerProduct: dot.
	InnerProduct
)

// DefaultMetric applies when the descriptor names none.
const DefaultMetric = Cosine

func (m Metric) String() string {
	switch m {
	case Cosine:
		return "cosine"
	case L2:
		return "l2"
	case InnerProduct:
		return "inner_product"
	default:
		return fmt.Sprintf("Metric(%d)", int(m))
	}
}

// ParseMetric parses the descriptor option spelling (cosine | l2 | inner_product).
func ParseMetric(s string) (Metric, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "cosine":
		return Cosine, nil
	case "l2", "euclidean":
		return L2, nil
	case "inner_product", "dot", "ip":
		return InnerProduct, nil
	default:
		return 0, fmt.Errorf("unknown vector metric: %q", s)
	}
}

// Score computes the metric in float64 over float32 inputs and rounds once at the end, so
// both trees produce the same float32 to well within the fixture tolerance.
func Score(m Metric, a, b []float32) float32 {
	return float32(score64(m, a, b))
}

func score64(m Metric, a, b []float32) float64 {
	switch m {
	case L2:
		var sum float64
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			sum += d * d
		}
		return 1 / (1 + math.Sqrt(sum))
	case InnerProduct:
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return dot
	default:
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		if na == 0 || nb == 0 {
			return 0
		}
		return dot / (math.Sqrt(na) * math.Sqrt(nb))
	}
}
