package auth

import "testing"

func TestHashAndVerifyPasswordRoundTrip(t *testing.T) {
	hash, err := HashPassword("correct-horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if err := VerifyPassword(hash, "correct-horse"); err != nil {
		t.Errorf("VerifyPassword: %v, want nil", err)
	}
}

func TestVerifyPasswordRejectsWrongPassword(t *testing.T) {
	hash, err := HashPassword("correct-horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if err := VerifyPassword(hash, "wrong-password"); err == nil {
		t.Error("VerifyPassword: want error for wrong password, got nil")
	}
}

func TestHashPasswordSaltsEachCall(t *testing.T) {
	h1, err := HashPassword("correct-horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	h2, err := HashPassword("correct-horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if h1 == h2 {
		t.Error("HashPassword: want different hashes for the same password (bcrypt salting), got identical")
	}
	if err := VerifyPassword(h1, "correct-horse"); err != nil {
		t.Errorf("VerifyPassword(h1): %v, want nil", err)
	}
	if err := VerifyPassword(h2, "correct-horse"); err != nil {
		t.Errorf("VerifyPassword(h2): %v, want nil", err)
	}
}
