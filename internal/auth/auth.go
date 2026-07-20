// Package auth provides password hashing and verification for goqd's
// username/password authentication.
package auth

import "golang.org/x/crypto/bcrypt"

// HashPassword returns a bcrypt hash of plain, suitable for storage.
func HashPassword(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// VerifyPassword returns nil if plain matches hash, and a non-nil error
// otherwise.
func VerifyPassword(hash, plain string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain))
}
