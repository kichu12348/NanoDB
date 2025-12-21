package collection

import "fmt"

func match(doc map[string]any, query map[string]any) bool {
	for key, queryVal := range query {

		if key == "$or" {
			if list, ok := queryVal.([]any); ok {
				if !matchOr(doc, list) {
					return false
				}
			}
			continue
		}

		if key == "$and" {
			if list, ok := queryVal.([]any); ok {
				if !matchAnd(doc, list) {
					return false
				}
			}
			continue
		}

		docVal, exists := doc[key]

		if !exists {
			return false
		}

		if opMap, ok := queryVal.(map[string]any); ok {
			if !matchOperators(docVal, opMap) {
				return false
			}
		} else {
			if !valueEqual(docVal, queryVal) {
				return false
			}
		}
	}
	return true
}

func matchOr(doc map[string]any, list []any) bool {
	for _, item := range list {
		if subQuery, ok := item.(map[string]any); ok {
			if match(doc, subQuery) {
				return true
			}
		}
	}
	return false
}

func matchAnd(doc map[string]any, list []any) bool {
	for _, item := range list {
		if subQuery, ok := item.(map[string]any); ok {
			if !match(doc, subQuery) {
				return false
			}
		}
	}
	return true
}

func matchOperators(docVal any, opMap map[string]any) bool {
	for op, targetVal := range opMap {
		switch op {
		case "$eq":
			if !valueEqual(docVal, targetVal) {
				return false
			}
		case "$ne":
			if valueEqual(docVal, targetVal) {
				return false
			}
		case "$gt":
			if compare(docVal, targetVal) <= 0 {
				return false
			}
		case "$gte":
			if compare(docVal, targetVal) < 0 {
				return false
			}
		case "$lt":
			if compare(docVal, targetVal) >= 0 {
				return false
			}
		case "$lte":
			if compare(docVal, targetVal) > 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func valueEqual(a, b any) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compare(a, b any) int {
	valA, okA := toFloat(a)
	valB, okB := toFloat(b)

	if okA && okB {
		if valA < valB {
			return -1
		}
		if valB < valA {
			return 1
		}
		return 0
	}

	// fallback to strings
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	if strA < strB {
		return -1
	}
	if strB < strA {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	default:
		return 0, false
	}
}
