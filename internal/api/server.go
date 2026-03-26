package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"emulator-aws-sns/internal/delivery"
	"emulator-aws-sns/internal/domain"
	"emulator-aws-sns/internal/service"
	"emulator-aws-sns/internal/signing"
)

const xmlNS = "https://sns.amazonaws.com/doc/2010-03-31/"

type Server struct {
	service   *service.Service
	signer    *signing.Provider
	accountID string
}

func New(service *service.Service, signer *signing.Provider, accountID string) *Server {
	return &Server{service: service, signer: signer, accountID: accountID}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/__sns/certs/current.pem", s.handleCert)
	return mux
}

func (s *Server) handleCert(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/x-pem-file")
	_, _ = w.Write(s.signer.CertPEM())
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.writeError(w, r, &domain.APIError{Code: "InvalidParameter", Message: "Unable to parse request", HTTPStatus: 400, Sender: true})
		return
	}
	action := strings.TrimSpace(r.Form.Get("Action"))
	if action == "" {
		s.writeError(w, r, &domain.APIError{Code: "InvalidParameter", Message: "Missing Action", HTTPStatus: 400, Sender: true})
		return
	}
	ctx := r.Context()
	identity := domain.Identity{AccountID: s.accountID, Principal: s.accountID}
	var err error
	switch action {
	case "CreateTopic":
		err = s.createTopic(ctx, w, r, identity)
	case "DeleteTopic":
		err = s.deleteTopic(ctx, w, r, identity)
	case "GetTopicAttributes":
		err = s.getTopicAttributes(ctx, w, r, identity)
	case "SetTopicAttributes":
		err = s.setTopicAttributes(ctx, w, r, identity)
	case "ListTopics":
		err = s.listTopics(ctx, w, r, identity)
	case "Subscribe":
		err = s.subscribe(ctx, w, r, identity)
	case "ConfirmSubscription":
		err = s.confirmSubscription(ctx, w, r, identity)
	case "Unsubscribe":
		err = s.unsubscribe(ctx, w, r, identity)
	case "GetSubscriptionAttributes":
		err = s.getSubscriptionAttributes(ctx, w, r, identity)
	case "SetSubscriptionAttributes":
		err = s.setSubscriptionAttributes(ctx, w, r, identity)
	case "ListSubscriptions":
		err = s.listSubscriptions(ctx, w, r, identity)
	case "ListSubscriptionsByTopic":
		err = s.listSubscriptionsByTopic(ctx, w, r, identity)
	case "Publish":
		err = s.publish(ctx, w, r, identity)
	case "PublishBatch":
		err = s.publishBatch(ctx, w, r, identity)
	case "TagResource":
		err = s.tagResource(ctx, w, r, identity)
	case "UntagResource":
		err = s.untagResource(ctx, w, r, identity)
	case "ListTagsForResource":
		err = s.listTags(ctx, w, r, identity)
	case "AddPermission":
		err = s.addPermission(ctx, w, r, identity)
	case "RemovePermission":
		err = s.removePermission(ctx, w, r, identity)
	default:
		err = &domain.APIError{Code: "InvalidAction", Message: "Unknown Action", HTTPStatus: 400, Sender: true}
	}
	if err != nil {
		var apiErr *domain.APIError
		if !errorAs(err, &apiErr) {
			apiErr = &domain.APIError{Code: "InternalError", Message: err.Error(), HTTPStatus: 500}
		}
		s.writeError(w, r, apiErr)
	}
}

func (s *Server) createTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	topic, _, err := s.service.CreateTopic(ctx, identity, r.Form.Get("Name"), parseAttributes(r))
	if err != nil {
		return err
	}
	return writeSuccess(w, "CreateTopicResponse", "CreateTopicResult", topicArnResult{TopicArn: topic.ARN})
}

func (s *Server) deleteTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.DeleteTopic(ctx, identity, r.Form.Get("TopicArn")); err != nil {
		return err
	}
	return writeSuccess(w, "DeleteTopicResponse", "", nil)
}

func (s *Server) getTopicAttributes(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	topic, err := s.service.GetTopic(ctx, identity, r.Form.Get("TopicArn"))
	if err != nil {
		return err
	}
	subscriptions, _, _ := s.service.ListSubscriptionsByTopic(ctx, identity, topic.ARN, "")
	confirmed, pending := 0, 0
	for _, sub := range subscriptions {
		if sub.PendingConfirmation {
			pending++
		} else {
			confirmed++
		}
	}
	attrs := topic.Attributes.AttributeMap()
	attrs["Owner"] = topic.Owner
	attrs["TopicArn"] = topic.ARN
	attrs["SubscriptionsConfirmed"] = strconv.Itoa(confirmed)
	attrs["SubscriptionsPending"] = strconv.Itoa(pending)
	attrs["SubscriptionsDeleted"] = "0"
	if _, effective, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, ""); err == nil {
		attrs["EffectiveDeliveryPolicy"] = effective
	}
	return writeSuccess(w, "GetTopicAttributesResponse", "GetTopicAttributesResult", getTopicAttributesResult{Attributes: newAttributeMap(attrs)})
}

func (s *Server) setTopicAttributes(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	_, err := s.service.SetTopicAttribute(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("AttributeName"), r.Form.Get("AttributeValue"))
	if err != nil {
		return err
	}
	return writeSuccess(w, "SetTopicAttributesResponse", "", nil)
}

func (s *Server) listTopics(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	topics, nextToken, err := s.service.ListTopics(ctx, identity, r.Form.Get("NextToken"))
	if err != nil {
		return err
	}
	result := listTopicsResult{NextToken: nextToken}
	for _, topic := range topics {
		result.Topics = append(result.Topics, topicMember{TopicArn: topic.ARN})
	}
	return writeSuccess(w, "ListTopicsResponse", "ListTopicsResult", result)
}

func (s *Server) subscribe(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	returnArn := strings.EqualFold(r.Form.Get("ReturnSubscriptionArn"), "true")
	subArn, err := s.service.Subscribe(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("Protocol"), r.Form.Get("Endpoint"), parseAttributes(r), returnArn)
	if err != nil {
		return err
	}
	return writeSuccess(w, "SubscribeResponse", "SubscribeResult", subscriptionArnResult{SubscriptionArn: subArn})
}

func (s *Server) confirmSubscription(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	authOnUnsubscribe := strings.EqualFold(r.Form.Get("AuthenticateOnUnsubscribe"), "true")
	subArn, err := s.service.ConfirmSubscription(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("Token"), authOnUnsubscribe)
	if err != nil {
		return err
	}
	return writeSuccess(w, "ConfirmSubscriptionResponse", "ConfirmSubscriptionResult", subscriptionArnResult{SubscriptionArn: subArn})
}

func (s *Server) unsubscribe(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.Unsubscribe(ctx, identity, r.Form.Get("SubscriptionArn")); err != nil {
		return err
	}
	return writeSuccess(w, "UnsubscribeResponse", "", nil)
}

func (s *Server) getSubscriptionAttributes(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	sub, _, err := s.service.GetSubscription(ctx, identity, r.Form.Get("SubscriptionArn"))
	if err != nil {
		return err
	}
	attrs := sub.Attributes.AttributeMap(sub)
	return writeSuccess(w, "GetSubscriptionAttributesResponse", "GetSubscriptionAttributesResult", getSubscriptionAttributesResult{Attributes: newAttributeMap(attrs)})
}

func (s *Server) setSubscriptionAttributes(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	_, err := s.service.SetSubscriptionAttribute(ctx, identity, r.Form.Get("SubscriptionArn"), r.Form.Get("AttributeName"), r.Form.Get("AttributeValue"))
	if err != nil {
		return err
	}
	return writeSuccess(w, "SetSubscriptionAttributesResponse", "", nil)
}

func (s *Server) listSubscriptions(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	items, nextToken, err := s.service.ListSubscriptions(ctx, identity, r.Form.Get("NextToken"))
	if err != nil {
		return err
	}
	return writeSubscriptionList(w, "ListSubscriptionsResponse", "ListSubscriptionsResult", items, nextToken)
}

func (s *Server) listSubscriptionsByTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	items, nextToken, err := s.service.ListSubscriptionsByTopic(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("NextToken"))
	if err != nil {
		return err
	}
	return writeSubscriptionList(w, "ListSubscriptionsByTopicResponse", "ListSubscriptionsByTopicResult", items, nextToken)
}

func (s *Server) publish(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	out, err := s.service.Publish(ctx, identity, parsePublishInput(r))
	if err != nil {
		return err
	}
	return writeSuccess(w, "PublishResponse", "PublishResult", publishResult{MessageID: out.MessageID, SequenceNumber: out.SequenceNumber})
}

func (s *Server) publishBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	success, failed, err := s.service.PublishBatch(ctx, identity, r.Form.Get("TopicArn"), parseBatchEntries(r))
	if err != nil {
		return err
	}
	result := publishBatchResult{}
	for _, item := range success {
		result.Successful = append(result.Successful, batchSuccessEntry{ID: item.ID, MessageID: item.MessageID, SequenceNumber: item.SequenceNumber})
	}
	for _, item := range failed {
		result.Failed = append(result.Failed, batchFailureEntry{ID: item.ID, Code: item.Code, Message: item.Message, SenderFault: item.SenderFault})
	}
	return writeSuccess(w, "PublishBatchResponse", "PublishBatchResult", result)
}

func (s *Server) tagResource(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.TagTopic(ctx, identity, r.Form.Get("ResourceArn"), parseTags(r)); err != nil {
		return err
	}
	return writeSuccess(w, "TagResourceResponse", "TagResourceResult", emptyResult{})
}

func (s *Server) untagResource(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.UntagTopic(ctx, identity, r.Form.Get("ResourceArn"), parseStringMembers(r.Form, "TagKeys.member.")); err != nil {
		return err
	}
	return writeSuccess(w, "UntagResourceResponse", "UntagResourceResult", emptyResult{})
}

func (s *Server) listTags(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	topic, err := s.service.GetTopic(ctx, identity, r.Form.Get("ResourceArn"))
	if err != nil {
		return err
	}
	result := listTagsResult{}
	for _, item := range service.SortedTags(topic.Tags) {
		result.Tags = append(result.Tags, tagMember{Key: item.Key, Value: item.Value})
	}
	return writeSuccess(w, "ListTagsForResourceResponse", "ListTagsForResourceResult", result)
}

func (s *Server) addPermission(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.AddPermission(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("Label"), parseStringMembers(r.Form, "AWSAccountId.member."), parseStringMembers(r.Form, "ActionName.member.")); err != nil {
		return err
	}
	return writeSuccess(w, "AddPermissionResponse", "", nil)
}

func (s *Server) removePermission(ctx context.Context, w http.ResponseWriter, r *http.Request, identity domain.Identity) error {
	if err := s.service.RemovePermission(ctx, identity, r.Form.Get("TopicArn"), r.Form.Get("Label")); err != nil {
		return err
	}
	return writeSuccess(w, "RemovePermissionResponse", "", nil)
}

type attributeEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type attributeMap struct {
	Entries []attributeEntry `xml:"entry"`
}

type emptyResult struct{}

type topicArnResult struct {
	TopicArn string `xml:"TopicArn"`
}

type subscriptionArnResult struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
}

type topicMember struct {
	TopicArn string `xml:"TopicArn"`
}

type listTopicsResult struct {
	Topics    []topicMember `xml:"Topics>member"`
	NextToken string        `xml:"NextToken,omitempty"`
}

type getTopicAttributesResult struct {
	Attributes attributeMap `xml:"Attributes"`
}

type getSubscriptionAttributesResult struct {
	Attributes attributeMap `xml:"Attributes"`
}

type publishResult struct {
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:"SequenceNumber,omitempty"`
}

type batchSuccessEntry struct {
	ID             string `xml:"Id"`
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:"SequenceNumber,omitempty"`
}

type batchFailureEntry struct {
	ID          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

type publishBatchResult struct {
	Successful []batchSuccessEntry `xml:"Successful>member,omitempty"`
	Failed     []batchFailureEntry `xml:"Failed>member,omitempty"`
}

type tagMember struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value,omitempty"`
}

type listTagsResult struct {
	Tags []tagMember `xml:"Tags>member"`
}

func newAttributeMap(values map[string]string) attributeMap {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := attributeMap{Entries: make([]attributeEntry, 0, len(keys))}
	for _, key := range keys {
		out.Entries = append(out.Entries, attributeEntry{Key: key, Value: values[key]})
	}
	return out
}

func writeSubscriptionList(w http.ResponseWriter, responseName, resultName string, items []*domain.Subscription, nextToken string) error {
	result := subscriptionListResult{NextToken: nextToken}
	for _, sub := range items {
		arn := sub.ARN
		if sub.PendingConfirmation {
			arn = "PendingConfirmation"
		}
		result.Subscriptions = append(result.Subscriptions, subscriptionMember{
			SubscriptionArn: arn,
			Owner:           sub.Owner,
			Protocol:        sub.Protocol,
			Endpoint:        sub.Endpoint,
			TopicArn:        sub.TopicARN,
		})
	}
	return writeSuccess(w, responseName, resultName, result)
}

type subscriptionMember struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
	Owner           string `xml:"Owner"`
	Protocol        string `xml:"Protocol"`
	Endpoint        string `xml:"Endpoint"`
	TopicArn        string `xml:"TopicArn"`
}

type subscriptionListResult struct {
	Subscriptions []subscriptionMember `xml:"Subscriptions>member"`
	NextToken     string               `xml:"NextToken,omitempty"`
}

func writeSuccess(w http.ResponseWriter, responseName, resultName string, result any) error {
	type metadata struct {
		XMLName   xml.Name `xml:"ResponseMetadata"`
		RequestID string   `xml:"RequestId"`
	}
	meta := metadata{RequestID: encodeID("request")}
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	fmt.Fprintf(&buf, "<%s xmlns=\"%s\">", responseName, xmlNS)
	if resultName != "" {
		fmt.Fprintf(&buf, "<%s>", resultName)
		if result != nil {
			body, err := marshalResult(result)
			if err != nil {
				return err
			}
			buf.Write(stripXMLRoot(body))
		}
		fmt.Fprintf(&buf, "</%s>", resultName)
	}
	metaBody, err := xml.Marshal(meta)
	if err != nil {
		return err
	}
	buf.Write(metaBody)
	fmt.Fprintf(&buf, "</%s>", responseName)
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
	return nil
}

func marshalResult(result any) ([]byte, error) {
	value := reflect.ValueOf(result)
	if value.Kind() == reflect.Pointer {
		return xml.Marshal(result)
	}
	ptr := reflect.New(value.Type())
	ptr.Elem().Set(value)
	return xml.Marshal(ptr.Interface())
}

func stripXMLRoot(body []byte) []byte {
	start := bytes.IndexByte(body, '>')
	end := bytes.LastIndexByte(body, '<')
	if start >= 0 && end > start {
		return body[start+1 : end]
	}
	return body
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, apiErr *domain.APIError) {
	type errEntry struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	payload := struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		XMLNS     string   `xml:"xmlns,attr"`
		Error     errEntry `xml:"Error"`
		RequestID string   `xml:"RequestId"`
	}{
		XMLNS: xmlNS,
		Error: errEntry{
			Type:    ternary(apiErr.Sender, "Sender", "Receiver"),
			Code:    apiErr.Code,
			Message: apiErr.Message,
		},
		RequestID: encodeID("request"),
	}
	body, _ := xml.Marshal(payload)
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(apiErr.HTTPStatus)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

func parseAttributes(r *http.Request) map[string]string {
	return parseKeyValueEntries(r.Form, "Attributes.entry.", ".key", ".value", "Attributes.member.", ".Name", ".Value")
}

func parseTags(r *http.Request) map[string]string {
	values := parseKeyValueEntries(r.Form, "Tags.member.", ".Key", ".Value", "Tags.Tag.", ".Key", ".Value")
	if len(values) == 0 {
		values = parseKeyValueEntries(r.Form, "Tags.entry.", ".key", ".value", "", "", "")
	}
	return values
}

func parseKeyValueEntries(values map[string][]string, prefix1, keySuffix1, valueSuffix1, prefix2, keySuffix2, valueSuffix2 string) map[string]string {
	type item struct {
		key string
		val string
	}
	items := map[string]*item{}
	for key, value := range values {
		switch {
		case strings.HasPrefix(key, prefix1) && strings.HasSuffix(key, keySuffix1):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, prefix1), keySuffix1)
			entry := items[idx]
			if entry == nil {
				entry = &item{}
				items[idx] = entry
			}
			entry.key = first(value)
		case strings.HasPrefix(key, prefix1) && strings.HasSuffix(key, valueSuffix1):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, prefix1), valueSuffix1)
			entry := items[idx]
			if entry == nil {
				entry = &item{}
				items[idx] = entry
			}
			entry.val = first(value)
		case prefix2 != "" && strings.HasPrefix(key, prefix2) && strings.HasSuffix(key, keySuffix2):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, prefix2), keySuffix2)
			entry := items[idx]
			if entry == nil {
				entry = &item{}
				items[idx] = entry
			}
			entry.key = first(value)
		case prefix2 != "" && strings.HasPrefix(key, prefix2) && strings.HasSuffix(key, valueSuffix2):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, prefix2), valueSuffix2)
			entry := items[idx]
			if entry == nil {
				entry = &item{}
				items[idx] = entry
			}
			entry.val = first(value)
		}
	}
	out := map[string]string{}
	for _, item := range items {
		if item.key != "" {
			out[item.key] = item.val
		}
	}
	return out
}

func parseStringMembers(values map[string][]string, prefix string) []string {
	indices := make([]int, 0)
	items := map[int]string{}
	for key, value := range values {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(key, prefix))
		if err != nil {
			continue
		}
		indices = append(indices, index)
		items[index] = first(value)
	}
	sort.Ints(indices)
	out := make([]string, 0, len(indices))
	for _, idx := range indices {
		out = append(out, items[idx])
	}
	return out
}

func parsePublishInput(r *http.Request) domain.PublishInput {
	return domain.PublishInput{
		TopicARN:               r.Form.Get("TopicArn"),
		Message:                r.Form.Get("Message"),
		Subject:                r.Form.Get("Subject"),
		MessageStructure:       r.Form.Get("MessageStructure"),
		MessageAttributes:      parseMessageAttributes(r),
		MessageGroupID:         r.Form.Get("MessageGroupId"),
		MessageDeduplicationID: r.Form.Get("MessageDeduplicationId"),
	}
}

func parseMessageAttributes(r *http.Request) map[string]domain.MessageAttributeValue {
	items := map[string]*domain.MessageAttributeValue{}
	names := map[string]string{}
	for key, values := range r.Form {
		switch {
		case strings.HasPrefix(key, "MessageAttributes.entry.") && strings.HasSuffix(key, ".Name"):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, "MessageAttributes.entry."), ".Name")
			names[idx] = first(values)
		case strings.HasPrefix(key, "MessageAttributes.entry.") && strings.HasSuffix(key, ".Value.DataType"):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, "MessageAttributes.entry."), ".Value.DataType")
			attr := ensureAttr(items, idx)
			attr.DataType = first(values)
		case strings.HasPrefix(key, "MessageAttributes.entry.") && strings.HasSuffix(key, ".Value.StringValue"):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, "MessageAttributes.entry."), ".Value.StringValue")
			attr := ensureAttr(items, idx)
			attr.StringValue = first(values)
		case strings.HasPrefix(key, "MessageAttributes.entry.") && strings.HasSuffix(key, ".Value.BinaryValue"):
			idx := strings.TrimSuffix(strings.TrimPrefix(key, "MessageAttributes.entry."), ".Value.BinaryValue")
			attr := ensureAttr(items, idx)
			attr.BinaryValue, _ = base64.StdEncoding.DecodeString(first(values))
		}
	}
	out := map[string]domain.MessageAttributeValue{}
	for idx, name := range names {
		if attr, ok := items[idx]; ok && name != "" {
			out[name] = attr.DeepCopy()
		}
	}
	return out
}

func parseBatchEntries(r *http.Request) []domain.PublishBatchEntry {
	indices := map[string]*domain.PublishBatchEntry{}
	for key, values := range r.Form {
		if !strings.HasPrefix(key, "PublishBatchRequestEntries.member.") {
			continue
		}
		rest := strings.TrimPrefix(key, "PublishBatchRequestEntries.member.")
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			continue
		}
		entry := indices[parts[0]]
		if entry == nil {
			entry = &domain.PublishBatchEntry{MessageAttributes: map[string]domain.MessageAttributeValue{}}
			indices[parts[0]] = entry
		}
		switch parts[1] {
		case "Id":
			entry.ID = first(values)
		case "Message":
			entry.Message = first(values)
		case "Subject":
			entry.Subject = first(values)
		case "MessageStructure":
			entry.MessageStructure = first(values)
		case "MessageGroupId":
			entry.MessageGroupID = first(values)
		case "MessageDeduplicationId":
			entry.MessageDeduplicationID = first(values)
		default:
			if !strings.HasPrefix(parts[1], "MessageAttributes.entry.") {
				continue
			}
			rest := strings.TrimPrefix(parts[1], "MessageAttributes.entry.")
			attrParts := strings.SplitN(rest, ".", 2)
			if len(attrParts) != 2 {
				continue
			}
			attrIndex := attrParts[0]
			field := attrParts[1]
			switch field {
			case "Name":
				entry.MessageAttributes["__name__:"+attrIndex] = domain.MessageAttributeValue{StringValue: first(values)}
			case "Value.DataType":
				attr := entry.MessageAttributes[attrIndex]
				attr.DataType = first(values)
				entry.MessageAttributes[attrIndex] = attr
			case "Value.StringValue":
				attr := entry.MessageAttributes[attrIndex]
				attr.StringValue = first(values)
				entry.MessageAttributes[attrIndex] = attr
			case "Value.BinaryValue":
				attr := entry.MessageAttributes[attrIndex]
				attr.BinaryValue, _ = base64.StdEncoding.DecodeString(first(values))
				entry.MessageAttributes[attrIndex] = attr
			}
		}
	}
	keys := make([]int, 0, len(indices))
	for idx := range indices {
		if n, err := strconv.Atoi(idx); err == nil {
			keys = append(keys, n)
		}
	}
	sort.Ints(keys)
	out := make([]domain.PublishBatchEntry, 0, len(keys))
	for _, idx := range keys {
		entry := *indices[strconv.Itoa(idx)]
		if len(entry.MessageAttributes) > 0 {
			resolved := map[string]domain.MessageAttributeValue{}
			for key, value := range entry.MessageAttributes {
				if strings.HasPrefix(key, "__name__:") {
					continue
				}
				nameHolder, ok := entry.MessageAttributes["__name__:"+key]
				if !ok || nameHolder.StringValue == "" {
					continue
				}
				resolved[nameHolder.StringValue] = value
			}
			entry.MessageAttributes = resolved
		}
		out = append(out, entry)
	}
	return out
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func ensureAttr(items map[string]*domain.MessageAttributeValue, idx string) *domain.MessageAttributeValue {
	if items[idx] == nil {
		items[idx] = &domain.MessageAttributeValue{}
	}
	return items[idx]
}

func ternary(ok bool, yes, no string) string {
	if ok {
		return yes
	}
	return no
}

func encodeID(prefix string) string {
	return prefix + "-" + strconv.FormatInt(timeNow().UnixNano(), 36)
}

func timeNow() time.Time {
	return time.Now().UTC()
}

func errorAs(err error, target **domain.APIError) bool {
	apiErr, ok := err.(*domain.APIError)
	if ok {
		*target = apiErr
		return true
	}
	return false
}
