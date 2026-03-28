package signing

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"strings"
	"time"

	"emulator-aws-sns/internal/domain"
)

type Provider struct {
	key       *rsa.PrivateKey
	certPEM   []byte
	certPath  string
	createdAt time.Time
}

func NewProvider(certPath string) (*Provider, error) {
	return NewProviderFromMaterial(certPath, domain.SigningMaterial{})
}

func NewProviderFromMaterial(certPath string, material domain.SigningMaterial) (*Provider, error) {
	if len(material.PrivateKeyPEM) == 0 || len(material.CertPEM) == 0 {
		return generateProvider(certPath)
	}
	key, err := parsePrivateKey(material.PrivateKeyPEM)
	if err != nil {
		return nil, err
	}
	createdAt := material.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return &Provider{
		key:       key,
		certPEM:   append([]byte(nil), material.CertPEM...),
		certPath:  certPath,
		createdAt: createdAt,
	}, nil
}

func generateProvider(certPath string) (*Provider, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject: pkix.Name{
			CommonName: "sns-emulator.local",
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return &Provider{
		key:       key,
		certPEM:   certPEM,
		certPath:  certPath,
		createdAt: now,
	}, nil
}

func (p *Provider) Material() (domain.SigningMaterial, error) {
	keyDER := x509.MarshalPKCS1PrivateKey(p.key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
	return domain.SigningMaterial{
		PrivateKeyPEM: keyPEM,
		CertPEM:       p.CertPEM(),
		CreatedAt:     p.createdAt,
	}, nil
}

func (p *Provider) CertPEM() []byte {
	return append([]byte(nil), p.certPEM...)
}

func (p *Provider) SigningCertURL(baseURL string) string {
	u, _ := url.Parse(strings.TrimRight(baseURL, "/"))
	u.Path = strings.TrimRight(u.Path, "/") + p.certPath
	return u.String()
}

func (p *Provider) Sign(attempt *domain.DeliveryAttempt) (string, error) {
	message := stringToSign(attempt)
	switch attempt.SignatureVersion {
	case "", "1":
		sum := sha1.Sum([]byte(message))
		sig, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA1, sum[:])
		if err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(sig), nil
	case "2":
		sum := sha256.Sum256([]byte(message))
		sig, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, sum[:])
		if err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(sig), nil
	default:
		return "", nil
	}
}

func stringToSign(attempt *domain.DeliveryAttempt) string {
	var fields []string
	switch attempt.Type {
	case "Notification":
		fields = append(fields, "Message", attempt.ProtocolMessage)
		fields = append(fields, "MessageId", attempt.MessageID)
		if attempt.Subject != "" {
			fields = append(fields, "Subject", attempt.Subject)
		}
		fields = append(fields, "Timestamp", domain.DefaultTimestamp(attempt.Timestamp))
		fields = append(fields, "TopicArn", attempt.Topic.ARN)
		fields = append(fields, "Type", attempt.Type)
	default:
		fields = append(fields, "Message", attempt.ProtocolMessage)
		fields = append(fields, "MessageId", attempt.MessageID)
		fields = append(fields, "SubscribeURL", attempt.SubscribeURL)
		fields = append(fields, "Timestamp", domain.DefaultTimestamp(attempt.Timestamp))
		fields = append(fields, "Token", attempt.Token)
		fields = append(fields, "TopicArn", attempt.Topic.ARN)
		fields = append(fields, "Type", attempt.Type)
	}
	return strings.Join(fields, "\n")
}

func parsePrivateKey(keyPEM []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("invalid private key pem")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return key, nil
}
