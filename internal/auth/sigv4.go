package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"emulator-aws-sns/internal/domain"
)

const (
	ModeStrict = "strict"
	ModeBypass = "bypass"
)

type Credential struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type Verifier struct {
	AccountID string
	Region    string
	Service   string
	Mode      string
	Now       func() time.Time
	creds     map[string]Credential
}

func NewVerifier(accountID, region, mode string, creds []Credential) *Verifier {
	lookup := make(map[string]Credential, len(creds))
	for _, cred := range creds {
		if cred.AccessKeyID == "" {
			continue
		}
		lookup[cred.AccessKeyID] = cred
	}
	return &Verifier{
		AccountID: accountID,
		Region:    region,
		Service:   "sns",
		Mode:      mode,
		Now: func() time.Time {
			return time.Now().UTC()
		},
		creds: lookup,
	}
}

func ParseStaticCredentials(raw string) ([]Credential, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []Credential{{AccessKeyID: "test", SecretAccessKey: "test", SessionToken: "test"}}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]Credential, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) < 2 || len(fields) > 3 {
			return nil, fmt.Errorf("invalid SNS_CREDENTIALS entry %q", part)
		}
		cred := Credential{AccessKeyID: fields[0], SecretAccessKey: fields[1]}
		if len(fields) == 3 {
			cred.SessionToken = fields[2]
		}
		out = append(out, cred)
	}
	return out, nil
}

func (v *Verifier) Verify(r *http.Request, rawBody []byte, allowAnonymous bool) (domain.Identity, error) {
	if strings.EqualFold(v.Mode, ModeBypass) {
		return v.identity(), nil
	}
	if hasHeaderAuth(r) {
		if err := v.verifyHeaderSignature(r, rawBody); err != nil {
			return domain.Identity{}, err
		}
		return v.identity(), nil
	}
	if hasQueryAuth(r.URL.Query()) {
		if err := v.verifyQuerySignature(r, rawBody); err != nil {
			return domain.Identity{}, err
		}
		return v.identity(), nil
	}
	if allowAnonymous {
		return v.identity(), nil
	}
	return domain.Identity{}, invalidSecurity("Request must be signed with AWS Signature Version 4")
}

func (v *Verifier) identity() domain.Identity {
	return domain.Identity{AccountID: v.AccountID, Principal: v.AccountID}
}

func (v *Verifier) verifyHeaderSignature(r *http.Request, rawBody []byte) error {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return invalidSecurity("Unsupported authorization algorithm")
	}
	fields := parseAuthFields(strings.TrimPrefix(authHeader, "AWS4-HMAC-SHA256 "))
	credField, ok := fields["Credential"]
	if !ok {
		return invalidSecurity("Missing Credential in Authorization header")
	}
	signedHeadersField, ok := fields["SignedHeaders"]
	if !ok {
		return invalidSecurity("Missing SignedHeaders in Authorization header")
	}
	signatureField, ok := fields["Signature"]
	if !ok {
		return invalidSecurity("Missing Signature in Authorization header")
	}
	scope, cred, err := v.resolveCredential(credField, r.Header.Get("X-Amz-Security-Token"))
	if err != nil {
		return err
	}
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return invalidSecurity("Missing X-Amz-Date header")
	}
	signedHeaders := strings.Split(signedHeadersField, ";")
	canonicalRequest, err := buildCanonicalRequest(r, rawBody, signedHeaders, false)
	if err != nil {
		return invalidSecurity(err.Error())
	}
	stringToSign := buildStringToSign(amzDate, scope, canonicalRequest)
	expected := hex.EncodeToString(hmacSHA256(signingKey(cred.SecretAccessKey, scope.Date, scope.Region, scope.Service), stringToSign))
	if !hmac.Equal([]byte(expected), []byte(signatureField)) {
		return invalidSecurity("The request signature we calculated does not match the signature you provided.")
	}
	return nil
}

func (v *Verifier) verifyQuerySignature(r *http.Request, rawBody []byte) error {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return invalidSecurity("Unsupported authorization algorithm")
	}
	scope, cred, err := v.resolveCredential(q.Get("X-Amz-Credential"), q.Get("X-Amz-Security-Token"))
	if err != nil {
		return err
	}
	amzDate := q.Get("X-Amz-Date")
	if amzDate == "" {
		return invalidSecurity("Missing X-Amz-Date query parameter")
	}
	expiresRaw := q.Get("X-Amz-Expires")
	if expiresRaw == "" {
		return invalidSecurity("Missing X-Amz-Expires query parameter")
	}
	expires, err := strconv.Atoi(expiresRaw)
	if err != nil || expires < 0 {
		return invalidSecurity("Invalid X-Amz-Expires query parameter")
	}
	ts, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return invalidSecurity("Invalid X-Amz-Date")
	}
	now := v.Now()
	if now.After(ts.Add(time.Duration(expires) * time.Second)) {
		return invalidSecurity("Signature expired")
	}
	signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	canonicalRequest, err := buildCanonicalRequest(r, rawBody, signedHeaders, true)
	if err != nil {
		return invalidSecurity(err.Error())
	}
	stringToSign := buildStringToSign(amzDate, scope, canonicalRequest)
	expected := hex.EncodeToString(hmacSHA256(signingKey(cred.SecretAccessKey, scope.Date, scope.Region, scope.Service), stringToSign))
	if !hmac.Equal([]byte(expected), []byte(q.Get("X-Amz-Signature"))) {
		return invalidSecurity("The request signature we calculated does not match the signature you provided.")
	}
	return nil
}

type credentialScope struct {
	Date    string
	Region  string
	Service string
	Term    string
}

func (v *Verifier) resolveCredential(rawCredential, requestToken string) (credentialScope, Credential, error) {
	parts := strings.Split(rawCredential, "/")
	if len(parts) != 5 {
		return credentialScope{}, Credential{}, invalidSecurity("Invalid credential scope")
	}
	cred, ok := v.creds[parts[0]]
	if !ok {
		return credentialScope{}, Credential{}, invalidSecurity("Unknown access key")
	}
	scope := credentialScope{Date: parts[1], Region: parts[2], Service: parts[3], Term: parts[4]}
	if scope.Region != v.Region || scope.Service != v.Service || scope.Term != "aws4_request" {
		return credentialScope{}, Credential{}, invalidSecurity("Credential scope does not match this endpoint")
	}
	if cred.SessionToken != "" && requestToken != cred.SessionToken {
		return credentialScope{}, Credential{}, invalidSecurity("The security token included in the request is invalid.")
	}
	return scope, cred, nil
}

func buildCanonicalRequest(r *http.Request, rawBody []byte, signedHeaders []string, stripQuerySignature bool) (string, error) {
	canonicalHeaders, err := buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}
	hashedPayload := sha256Hex(rawBody)
	if len(rawBody) == 0 {
		hashedPayload = sha256Hex(nil)
	}
	return strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonicalQuery(r.URL.Query(), stripQuerySignature),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		hashedPayload,
	}, "\n"), nil
}

func buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	headers := make([]string, 0, len(signedHeaders))
	for _, name := range signedHeaders {
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower == "" {
			continue
		}
		value := ""
		if lower == "host" {
			value = r.Host
		} else {
			values := r.Header.Values(http.CanonicalHeaderKey(lower))
			if len(values) == 0 {
				values = r.Header.Values(lower)
			}
			if len(values) == 0 {
				return "", fmt.Errorf("signed header %q missing from request", lower)
			}
			for i := range values {
				values[i] = normalizeHeaderValue(values[i])
			}
			value = strings.Join(values, ",")
		}
		headers = append(headers, lower+":"+normalizeHeaderValue(value))
	}
	return strings.Join(headers, "\n") + "\n", nil
}

func canonicalURI(u *url.URL) string {
	if u == nil || u.Path == "" {
		return "/"
	}
	return u.EscapedPath()
}

func canonicalQuery(values url.Values, stripSignature bool) string {
	keys := make([]string, 0)
	for key := range values {
		if stripSignature && key == "X-Amz-Signature" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		for _, value := range vals {
			parts = append(parts, awsQueryEscape(key)+"="+awsQueryEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func awsQueryEscape(v string) string {
	replacer := strings.NewReplacer("+", "%20", "*", "%2A", "%7E", "~")
	return replacer.Replace(url.QueryEscape(v))
}

func buildStringToSign(amzDate string, scope credentialScope, canonicalRequest string) string {
	return strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		strings.Join([]string{scope.Date, scope.Region, scope.Service, scope.Term}, "/"),
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = io.Copy(mac, bytes.NewBufferString(value))
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func parseAuthFields(raw string) map[string]string {
	fields := map[string]string{}
	for part := range strings.SplitSeq(raw, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		fields[key] = value
	}
	return fields
}

func normalizeHeaderValue(v string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(v)), " ")
}

func hasHeaderAuth(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get("Authorization")) != ""
}

func hasQueryAuth(q url.Values) bool {
	return q.Get("X-Amz-Algorithm") != "" || q.Get("X-Amz-Credential") != ""
}

func invalidSecurity(message string) *domain.APIError {
	return &domain.APIError{Code: "InvalidSecurity", Message: message, HTTPStatus: http.StatusForbidden, Sender: true}
}
