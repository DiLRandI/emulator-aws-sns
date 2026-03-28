package policy

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Document struct {
	Version   string      `json:"Version,omitempty"`
	Statement []Statement `json:"Statement"`
}

type Statement struct {
	Sid       string                    `json:"Sid,omitempty"`
	Effect    string                    `json:"Effect"`
	Principal json.RawMessage           `json:"Principal,omitempty"`
	Action    json.RawMessage           `json:"Action"`
	Resource  json.RawMessage           `json:"Resource"`
	Condition map[string]map[string]any `json:"Condition,omitempty"`
}

type Request struct {
	Principal        string
	ServicePrincipal string
	Action           string
	Resource         string
	Context          map[string]string
}

func DefaultTopicPolicy(topicARN, accountID string) string {
	doc := Document{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:       "__default_statement_ID",
				Effect:    "Allow",
				Principal: mustJSON(map[string]any{"AWS": "*"}),
				Action: mustJSON([]string{
					"SNS:AddPermission", "SNS:RemovePermission", "SNS:DeleteTopic",
					"SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:Publish",
					"SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:TagResource",
					"SNS:UntagResource", "SNS:ListTagsForResource",
				}),
				Resource: mustJSON(topicARN),
				Condition: map[string]map[string]any{
					"StringEquals": {
						"AWS:SourceOwner": accountID,
					},
				},
			},
		},
	}
	data, _ := json.Marshal(doc)
	return string(data)
}

func Parse(raw string) (Document, error) {
	if strings.TrimSpace(raw) == "" {
		return Document{}, nil
	}
	var doc Document
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return Document{}, err
	}
	return doc, nil
}

func Marshal(doc Document) string {
	data, _ := json.Marshal(doc)
	return string(data)
}

func AddPermission(raw, label string, accountIDs, actionNames []string, topicARN string) (string, error) {
	doc, err := Parse(raw)
	if err != nil {
		return "", err
	}
	for _, stmt := range doc.Statement {
		if stmt.Sid == label {
			return "", fmt.Errorf("statement with label already exists")
		}
	}
	actions := make([]string, 0, len(actionNames))
	for _, action := range actionNames {
		action = strings.TrimSpace(action)
		if action == "" {
			continue
		}
		if !strings.HasPrefix(strings.ToUpper(action), "SNS:") {
			action = "SNS:" + action
		}
		actions = append(actions, action)
	}
	stmt := Statement{
		Sid:       label,
		Effect:    "Allow",
		Principal: mustJSON(map[string]any{"AWS": accountIDs}),
		Action:    mustJSON(actions),
		Resource:  mustJSON(topicARN),
	}
	doc.Statement = append(doc.Statement, stmt)
	return Marshal(doc), nil
}

func RemovePermission(raw, label string) (string, error) {
	doc, err := Parse(raw)
	if err != nil {
		return "", err
	}
	filtered := make([]Statement, 0, len(doc.Statement))
	found := false
	for _, stmt := range doc.Statement {
		if stmt.Sid == label {
			found = true
			continue
		}
		filtered = append(filtered, stmt)
	}
	if !found {
		return "", fmt.Errorf("statement not found")
	}
	doc.Statement = filtered
	return Marshal(doc), nil
}

func Evaluate(raw string, req Request, ownerAccountID string) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return req.Principal == ownerAccountID, nil
	}
	doc, err := Parse(raw)
	if err != nil {
		return false, err
	}
	allowed := req.Principal == ownerAccountID
	for _, stmt := range doc.Statement {
		matches, err := statementMatches(stmt, req)
		if err != nil || !matches {
			continue
		}
		if strings.EqualFold(stmt.Effect, "Deny") {
			return false, nil
		}
		if strings.EqualFold(stmt.Effect, "Allow") {
			allowed = true
		}
	}
	return allowed, nil
}

func statementMatches(stmt Statement, req Request) (bool, error) {
	actions, err := parseStringList(stmt.Action)
	if err != nil {
		return false, err
	}
	if !matchesAny(actions, req.Action) {
		return false, nil
	}
	resources, err := parseStringList(stmt.Resource)
	if err != nil {
		return false, err
	}
	if !matchesAny(resources, req.Resource) {
		return false, nil
	}
	if len(stmt.Principal) > 0 {
		ok, err := principalMatches(stmt.Principal, req)
		if err != nil || !ok {
			return false, err
		}
	}
	return conditionsMatch(stmt.Condition, req.Context), nil
}

func principalMatches(raw json.RawMessage, req Request) (bool, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s == "*" || s == req.Principal || s == req.ServicePrincipal, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return false, err
	}
	for key, value := range m {
		switch key {
		case "AWS":
			items, err := anyToStrings(value)
			if err != nil {
				return false, err
			}
			if matchesAny(items, req.Principal) {
				return true, nil
			}
		case "Service":
			items, err := anyToStrings(value)
			if err != nil {
				return false, err
			}
			if matchesAny(items, req.ServicePrincipal) {
				return true, nil
			}
		}
	}
	return false, nil
}

func conditionsMatch(conds map[string]map[string]any, ctx map[string]string) bool {
	for operator, values := range conds {
		switch operator {
		case "StringEquals", "ArnEquals":
			for key, value := range values {
				candidates, _ := anyToStrings(value)
				if !matchesAny(candidates, ctx[key]) {
					return false
				}
			}
		}
	}
	return true
}

func parseStringList(raw json.RawMessage) ([]string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []string{s}, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		return list, nil
	}
	return nil, fmt.Errorf("invalid list")
}

func anyToStrings(v any) ([]string, error) {
	switch typed := v.(type) {
	case string:
		return []string{typed}, nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("non-string value")
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported value")
	}
}

func matchesAny(items []string, value string) bool {
	for _, item := range items {
		if item == "*" || strings.EqualFold(item, value) || item == value {
			return true
		}
	}
	return false
}

func mustJSON(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
