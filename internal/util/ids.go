package util

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
)

func UUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	)
}

func RandomHex(n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, (n+1)/2)
	_, _ = rand.Read(buf)
	out := hex.EncodeToString(buf)
	return out[:n]
}

func RandomBigInt(bits int) *big.Int {
	max := new(big.Int).Lsh(big.NewInt(1), uint(bits))
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return big.NewInt(0)
	}
	return n
}
