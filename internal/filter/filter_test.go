package filter

import (
	"testing"

	"emulator-aws-sns/internal/domain"
)

func TestMatchesAttributeScope(t *testing.T) {
	attrs := map[string]domain.MessageAttributeValue{
		"store":     {DataType: "String", StringValue: "example_corp"},
		"event":     {DataType: "String", StringValue: "order_placed"},
		"price_usd": {DataType: "Number", StringValue: "210.75"},
		"interests": {DataType: "String.Array", StringValue: `["rugby","soccer"]`},
	}
	policy := `{
		"store": ["example_corp"],
		"event": [{"anything-but": "order_cancelled"}],
		"price_usd": [{"numeric": [">=", 100]}],
		"interests": ["rugby"]
	}`
	match, err := Matches(policy, domain.FilterScopeMessageAttributes, "", attrs)
	if err != nil {
		t.Fatalf("Matches() error = %v", err)
	}
	if !match {
		t.Fatalf("expected policy to match attributes")
	}
}

func TestMatchesBodyScopeWithNestedORAndExists(t *testing.T) {
	policy := `{
		"$or": [
			{"type": ["order.created"]},
			{"type": ["order.updated"]}
		],
		"detail": {
			"source": [{"prefix": "checkout-"}],
			"discount": [{"exists": false}]
		}
	}`
	body := `{"type":"order.created","detail":{"source":"checkout-web"}}`
	match, err := Matches(policy, domain.FilterScopeMessageBody, body, nil)
	if err != nil {
		t.Fatalf("Matches() error = %v", err)
	}
	if !match {
		t.Fatalf("expected policy to match body")
	}
}

func TestMatchesBodyScopeRejectsInvalidJSON(t *testing.T) {
	match, err := Matches(`{"event":["ok"]}`, domain.FilterScopeMessageBody, `not-json`, nil)
	if err != nil {
		t.Fatalf("Matches() unexpected error = %v", err)
	}
	if match {
		t.Fatalf("expected invalid JSON body to fail matching")
	}
}
