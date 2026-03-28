package delivery

import (
	"encoding/json"
	"fmt"
	"time"
)

type RetryPolicy struct {
	MinDelayTarget     int    `json:"minDelayTarget"`
	MaxDelayTarget     int    `json:"maxDelayTarget"`
	NumRetries         int    `json:"numRetries"`
	NumNoDelayRetries  int    `json:"numNoDelayRetries"`
	NumMinDelayRetries int    `json:"numMinDelayRetries"`
	NumMaxDelayRetries int    `json:"numMaxDelayRetries"`
	BackoffFunction    string `json:"backoffFunction"`
}

type ThrottlePolicy struct {
	MaxReceivesPerSecond int `json:"maxReceivesPerSecond"`
}

type RequestPolicy struct {
	HeaderContentType string `json:"headerContentType"`
}

type SubscriptionPolicy struct {
	HealthyRetryPolicy RetryPolicy    `json:"healthyRetryPolicy"`
	ThrottlePolicy     ThrottlePolicy `json:"throttlePolicy"`
	RequestPolicy      RequestPolicy  `json:"requestPolicy"`
}

type TopicPolicyEnvelope struct {
	HTTP struct {
		DefaultHealthyRetryPolicy    RetryPolicy    `json:"defaultHealthyRetryPolicy"`
		DisableSubscriptionOverrides bool           `json:"disableSubscriptionOverrides"`
		DefaultThrottlePolicy        ThrottlePolicy `json:"defaultThrottlePolicy"`
		DefaultRequestPolicy         RequestPolicy  `json:"defaultRequestPolicy"`
	} `json:"http"`
}

func DefaultPolicy() SubscriptionPolicy {
	return SubscriptionPolicy{
		HealthyRetryPolicy: RetryPolicy{
			MinDelayTarget:     20,
			MaxDelayTarget:     20,
			NumRetries:         3,
			NumNoDelayRetries:  0,
			NumMinDelayRetries: 0,
			NumMaxDelayRetries: 0,
			BackoffFunction:    "linear",
		},
		RequestPolicy: RequestPolicy{
			HeaderContentType: "text/plain; charset=UTF-8",
		},
	}
}

func ParseTopicPolicy(raw string) (TopicPolicyEnvelope, error) {
	if raw == "" {
		env := TopicPolicyEnvelope{}
		env.HTTP.DefaultHealthyRetryPolicy = DefaultPolicy().HealthyRetryPolicy
		env.HTTP.DefaultRequestPolicy = DefaultPolicy().RequestPolicy
		return env, nil
	}
	var env TopicPolicyEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return TopicPolicyEnvelope{}, err
	}
	return env, nil
}

func ParseSubscriptionPolicy(raw string) (SubscriptionPolicy, error) {
	if raw == "" {
		return DefaultPolicy(), nil
	}
	var out SubscriptionPolicy
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return SubscriptionPolicy{}, err
	}
	mergeDefaults(&out)
	return out, nil
}

func EffectivePolicy(topicRaw, subRaw string) (SubscriptionPolicy, string, error) {
	base := DefaultPolicy()
	topicEnv, err := ParseTopicPolicy(topicRaw)
	if err != nil {
		return SubscriptionPolicy{}, "", err
	}
	base.HealthyRetryPolicy = mergeRetryPolicy(base.HealthyRetryPolicy, topicEnv.HTTP.DefaultHealthyRetryPolicy)
	base.ThrottlePolicy = mergeThrottlePolicy(base.ThrottlePolicy, topicEnv.HTTP.DefaultThrottlePolicy)
	base.RequestPolicy = mergeRequestPolicy(base.RequestPolicy, topicEnv.HTTP.DefaultRequestPolicy)
	if !topicEnv.HTTP.DisableSubscriptionOverrides && subRaw != "" {
		sub, err := ParseSubscriptionPolicy(subRaw)
		if err != nil {
			return SubscriptionPolicy{}, "", err
		}
		base.HealthyRetryPolicy = mergeRetryPolicy(base.HealthyRetryPolicy, sub.HealthyRetryPolicy)
		base.ThrottlePolicy = mergeThrottlePolicy(base.ThrottlePolicy, sub.ThrottlePolicy)
		base.RequestPolicy = mergeRequestPolicy(base.RequestPolicy, sub.RequestPolicy)
	}
	data, _ := json.Marshal(base)
	return base, string(data), nil
}

func mergeDefaults(out *SubscriptionPolicy) {
	base := DefaultPolicy()
	out.HealthyRetryPolicy = mergeRetryPolicy(base.HealthyRetryPolicy, out.HealthyRetryPolicy)
	out.ThrottlePolicy = mergeThrottlePolicy(base.ThrottlePolicy, out.ThrottlePolicy)
	out.RequestPolicy = mergeRequestPolicy(base.RequestPolicy, out.RequestPolicy)
}

func mergeRetryPolicy(base, override RetryPolicy) RetryPolicy {
	out := base
	if override.MinDelayTarget != 0 {
		out.MinDelayTarget = override.MinDelayTarget
	}
	if override.MaxDelayTarget != 0 {
		out.MaxDelayTarget = override.MaxDelayTarget
	}
	if override.NumRetries != 0 || override.NumNoDelayRetries != 0 || override.NumMinDelayRetries != 0 || override.NumMaxDelayRetries != 0 {
		out.NumRetries = override.NumRetries
		out.NumNoDelayRetries = override.NumNoDelayRetries
		out.NumMinDelayRetries = override.NumMinDelayRetries
		out.NumMaxDelayRetries = override.NumMaxDelayRetries
	}
	if override.BackoffFunction != "" {
		out.BackoffFunction = override.BackoffFunction
	}
	if out.MinDelayTarget == 0 {
		out.MinDelayTarget = 20
	}
	if out.MaxDelayTarget == 0 {
		out.MaxDelayTarget = out.MinDelayTarget
	}
	if out.BackoffFunction == "" {
		out.BackoffFunction = "linear"
	}
	return out
}

func mergeThrottlePolicy(base, override ThrottlePolicy) ThrottlePolicy {
	if override.MaxReceivesPerSecond != 0 {
		base.MaxReceivesPerSecond = override.MaxReceivesPerSecond
	}
	return base
}

func mergeRequestPolicy(base, override RequestPolicy) RequestPolicy {
	if override.HeaderContentType != "" {
		base.HeaderContentType = override.HeaderContentType
	}
	return base
}

func ValidateSubscriptionPolicy(raw string) error {
	_, err := ParseSubscriptionPolicy(raw)
	return err
}

func ValidateTopicPolicy(raw string) error {
	_, err := ParseTopicPolicy(raw)
	return err
}

func RetryDelays(policy SubscriptionPolicy) ([]time.Duration, error) {
	rp := policy.HealthyRetryPolicy
	if rp.NumRetries < 0 || rp.NumRetries > 100 {
		return nil, fmt.Errorf("numRetries must be between 0 and 100")
	}
	if rp.MinDelayTarget <= 0 || rp.MaxDelayTarget <= 0 || rp.MinDelayTarget > rp.MaxDelayTarget {
		return nil, fmt.Errorf("invalid delay configuration")
	}
	backoffRetries := max(rp.NumRetries-rp.NumNoDelayRetries-rp.NumMinDelayRetries-rp.NumMaxDelayRetries, 0)
	var out []time.Duration
	for i := 0; i < rp.NumNoDelayRetries; i++ {
		out = append(out, 0)
	}
	for i := 0; i < rp.NumMinDelayRetries; i++ {
		out = append(out, time.Duration(rp.MinDelayTarget)*time.Second)
	}
	for i := 0; i < backoffRetries; i++ {
		out = append(out, time.Duration(backoffDelay(rp, i, backoffRetries))*time.Second)
	}
	for i := 0; i < rp.NumMaxDelayRetries; i++ {
		out = append(out, time.Duration(rp.MaxDelayTarget)*time.Second)
	}
	return out, nil
}

func backoffDelay(policy RetryPolicy, attempt, total int) int {
	if total <= 1 {
		return policy.MinDelayTarget
	}
	switch policy.BackoffFunction {
	case "arithmetic", "linear":
		step := float64(policy.MaxDelayTarget-policy.MinDelayTarget) / float64(total-1)
		return int(float64(policy.MinDelayTarget) + step*float64(attempt))
	case "geometric":
		if attempt == 0 {
			return policy.MinDelayTarget
		}
		value := float64(policy.MinDelayTarget) * float64(attempt+1)
		if int(value) > policy.MaxDelayTarget {
			return policy.MaxDelayTarget
		}
		return int(value)
	case "exponential":
		value := float64(policy.MinDelayTarget)
		for range attempt {
			value *= 2
			if int(value) >= policy.MaxDelayTarget {
				return policy.MaxDelayTarget
			}
		}
		return int(value)
	default:
		return policy.MinDelayTarget
	}
}
