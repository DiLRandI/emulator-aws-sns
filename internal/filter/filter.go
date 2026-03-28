package filter

import (
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"regexp"
	"strconv"
	"strings"

	"emulator-aws-sns/internal/domain"
)

func Matches(policyJSON, scope, message string, attrs map[string]domain.MessageAttributeValue) (bool, error) {
	if strings.TrimSpace(policyJSON) == "" {
		return true, nil
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return false, err
	}
	if strings.TrimSpace(scope) == "" {
		scope = domain.FilterScopeMessageAttributes
	}
	switch scope {
	case domain.FilterScopeMessageAttributes:
		root := attributeRoot(attrs)
		return evalObject(policy, root, false)
	case domain.FilterScopeMessageBody:
		var body any
		if err := json.Unmarshal([]byte(message), &body); err != nil {
			return false, nil
		}
		root, ok := body.(map[string]any)
		if !ok {
			return false, nil
		}
		return evalObject(policy, root, true)
	default:
		return false, fmt.Errorf("unsupported filter scope")
	}
}

func Validate(policyJSON string) error {
	if strings.TrimSpace(policyJSON) == "" {
		return nil
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return err
	}
	return validateObject(policy)
}

func attributeRoot(attrs map[string]domain.MessageAttributeValue) map[string]any {
	out := map[string]any{}
	for key, attr := range attrs {
		switch attr.DataType {
		case "String":
			out[key] = attr.StringValue
		case "String.Array":
			var items []any
			if err := json.Unmarshal([]byte(attr.StringValue), &items); err == nil {
				out[key] = items
			} else {
				out[key] = []any{attr.StringValue}
			}
		case "Number":
			if n, err := strconv.ParseFloat(attr.StringValue, 64); err == nil {
				out[key] = n
			}
		default:
			out[key] = attr.String()
		}
	}
	return out
}

func evalObject(policy, candidate map[string]any, nested bool) (bool, error) {
	orMatched := true
	for key, value := range policy {
		if key == "$or" {
			ok, err := evalOR(value, candidate, nested)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			orMatched = ok
			continue
		}
		child, exists := candidate[key]
		switch typed := value.(type) {
		case map[string]any:
			obj, ok := child.(map[string]any)
			if !ok {
				return false, nil
			}
			match, err := evalObject(typed, obj, true)
			if err != nil || !match {
				return false, err
			}
		case []any:
			match, err := evalConditions(typed, child, exists)
			if err != nil || !match {
				return false, err
			}
		default:
			return false, fmt.Errorf("invalid filter policy shape")
		}
	}
	return orMatched, nil
}

func validateObject(policy map[string]any) error {
	for key, value := range policy {
		if key == "$or" {
			items, ok := value.([]any)
			if !ok || len(items) < 2 {
				return fmt.Errorf("$or must contain at least two objects")
			}
			for _, item := range items {
				obj, ok := item.(map[string]any)
				if !ok {
					return fmt.Errorf("$or entries must be objects")
				}
				if err := validateObject(obj); err != nil {
					return err
				}
			}
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			if err := validateObject(typed); err != nil {
				return err
			}
		case []any:
			if err := validateConditions(typed); err != nil {
				return err
			}
		default:
			return fmt.Errorf("filter policy values must be objects or arrays")
		}
	}
	return nil
}

func validateConditions(conditions []any) error {
	for _, condition := range conditions {
		switch typed := condition.(type) {
		case string, float64, bool, nil:
			continue
		case map[string]any:
			if err := validateOperator(typed); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported filter condition")
		}
	}
	return nil
}

func validateOperator(operator map[string]any) error {
	if len(operator) != 1 {
		return fmt.Errorf("operator object must contain exactly one operator")
	}
	for key, value := range operator {
		switch key {
		case "anything-but":
			switch typed := value.(type) {
			case string, float64, bool, nil:
				return nil
			case []any:
				return nil
			case map[string]any:
				if len(typed) != 1 {
					return fmt.Errorf("anything-but supports one nested operator")
				}
				for _, key := range []string{"prefix", "suffix", "equals-ignore-case", "wildcard", "cidr"} {
					if _, ok := typed[key]; ok {
						return validateOperator(typed)
					}
				}
				return fmt.Errorf("unsupported anything-but nested operator")
			default:
				return fmt.Errorf("unsupported anything-but value")
			}
		case "prefix", "suffix", "equals-ignore-case", "wildcard", "cidr":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s must be a string", key)
			}
		case "numeric":
			items, ok := value.([]any)
			if !ok || len(items) < 2 || len(items)%2 != 0 {
				return fmt.Errorf("numeric must contain operator/value pairs")
			}
		case "exists":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("exists must be boolean")
			}
		default:
			return fmt.Errorf("unsupported operator %q", key)
		}
	}
	return nil
}

func evalOR(raw any, candidate map[string]any, nested bool) (bool, error) {
	items, ok := raw.([]any)
	if !ok || len(items) < 2 {
		return false, nil
	}
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return false, fmt.Errorf("invalid $or item")
		}
		match, err := evalObject(obj, candidate, nested)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func evalConditions(conditions []any, candidate any, exists bool) (bool, error) {
	for _, condition := range conditions {
		match, err := evalCondition(condition, candidate, exists)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func evalCondition(condition, candidate any, exists bool) (bool, error) {
	switch typed := condition.(type) {
	case string, float64, bool, nil:
		return scalarOrArrayContains(candidate, typed), nil
	case map[string]any:
		if existsValue, ok := typed["exists"]; ok {
			want, _ := existsValue.(bool)
			return exists == want, nil
		}
		if anything, ok := typed["anything-but"]; ok {
			return evalAnythingBut(anything, candidate), nil
		}
		if prefix, ok := typed["prefix"]; ok {
			return evalPrefix(prefix, candidate), nil
		}
		if suffix, ok := typed["suffix"]; ok {
			return evalSuffix(suffix, candidate), nil
		}
		if equalsIgnoreCase, ok := typed["equals-ignore-case"]; ok {
			return evalEqualsIgnoreCase(equalsIgnoreCase, candidate), nil
		}
		if wildcard, ok := typed["wildcard"]; ok {
			return evalWildcard(wildcard, candidate)
		}
		if cidr, ok := typed["cidr"]; ok {
			return evalCIDR(cidr, candidate)
		}
		if numeric, ok := typed["numeric"]; ok {
			return evalNumeric(numeric, candidate)
		}
		return false, nil
	default:
		return false, nil
	}
}

func evalAnythingBut(rule, candidate any) bool {
	switch typed := rule.(type) {
	case string, float64, bool, nil:
		return !scalarOrArrayContains(candidate, typed)
	case []any:
		for _, item := range typed {
			if scalarOrArrayContains(candidate, item) {
				return false
			}
		}
		return true
	case map[string]any:
		if prefix, ok := typed["prefix"]; ok {
			return !evalPrefix(prefix, candidate)
		}
		if suffix, ok := typed["suffix"]; ok {
			return !evalSuffix(suffix, candidate)
		}
		if equalsIgnoreCase, ok := typed["equals-ignore-case"]; ok {
			return !evalEqualsIgnoreCase(equalsIgnoreCase, candidate)
		}
		if wildcard, ok := typed["wildcard"]; ok {
			match, _ := evalWildcard(wildcard, candidate)
			return !match
		}
		if cidr, ok := typed["cidr"]; ok {
			match, _ := evalCIDR(cidr, candidate)
			return !match
		}
	}
	return false
}

func evalPrefix(rule, candidate any) bool {
	want, ok := rule.(string)
	if !ok {
		return false
	}
	for _, value := range flatten(candidate) {
		s, ok := value.(string)
		if ok && strings.HasPrefix(s, want) {
			return true
		}
	}
	return false
}

func evalSuffix(rule, candidate any) bool {
	want, ok := rule.(string)
	if !ok {
		return false
	}
	for _, value := range flatten(candidate) {
		s, ok := value.(string)
		if ok && strings.HasSuffix(s, want) {
			return true
		}
	}
	return false
}

func evalEqualsIgnoreCase(rule, candidate any) bool {
	want, ok := rule.(string)
	if !ok {
		return false
	}
	for _, value := range flatten(candidate) {
		s, ok := value.(string)
		if ok && strings.EqualFold(s, want) {
			return true
		}
	}
	return false
}

func evalWildcard(rule, candidate any) (bool, error) {
	pattern, ok := rule.(string)
	if !ok {
		return false, fmt.Errorf("wildcard must be a string")
	}
	re, err := wildcardRegexp(pattern)
	if err != nil {
		return false, err
	}
	for _, value := range flatten(candidate) {
		s, ok := value.(string)
		if ok && re.MatchString(s) {
			return true, nil
		}
	}
	return false, nil
}

func evalCIDR(rule, candidate any) (bool, error) {
	raw, ok := rule.(string)
	if !ok {
		return false, fmt.Errorf("cidr must be a string")
	}
	prefix, err := netip.ParsePrefix(raw)
	if err != nil {
		return false, err
	}
	for _, value := range flatten(candidate) {
		s, ok := value.(string)
		if !ok {
			continue
		}
		addr, err := netip.ParseAddr(s)
		if err == nil && prefix.Contains(addr) {
			return true, nil
		}
	}
	return false, nil
}

func evalNumeric(rule, candidate any) (bool, error) {
	clauses, ok := rule.([]any)
	if !ok || len(clauses)%2 != 0 {
		return false, fmt.Errorf("invalid numeric clause")
	}
	values := flatten(candidate)
	for _, value := range values {
		number, ok := toFloat(value)
		if !ok {
			continue
		}
		match := true
		for i := 0; i < len(clauses); i += 2 {
			op, _ := clauses[i].(string)
			compare, ok := toFloat(clauses[i+1])
			if !ok {
				return false, fmt.Errorf("invalid numeric compare")
			}
			if !compareNumeric(number, op, compare) {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func compareNumeric(value float64, op string, compare float64) bool {
	switch op {
	case "=":
		return math.Abs(value-compare) < 1e-9
	case ">":
		return value > compare
	case ">=":
		return value >= compare
	case "<":
		return value < compare
	case "<=":
		return value <= compare
	default:
		return false
	}
}

func scalarOrArrayContains(candidate, expected any) bool {
	for _, item := range flatten(candidate) {
		if equal(item, expected) {
			return true
		}
	}
	return false
}

func flatten(candidate any) []any {
	switch typed := candidate.(type) {
	case []any:
		return typed
	default:
		return []any{candidate}
	}
}

func equal(a, b any) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := toFloat(b)
		return ok && math.Abs(av-bv) < 1e-9
	default:
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

func toFloat(v any) (float64, bool) {
	switch typed := v.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		n, err := typed.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(typed, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func wildcardRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
