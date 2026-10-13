package auth

import "testing"

func TestPasswordHash(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if hash == "correct horse battery staple" || !VerifyPassword("correct horse battery staple", hash) || VerifyPassword("wrong", hash) {
		t.Fatal("password verification failed")
	}
	if VerifyPassword("anything", "malformed") {
		t.Fatal("malformed hash accepted")
	}
}

func TestTokensAreOpaqueAndHashed(t *testing.T) {
	first, firstHash, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first == string(firstHash) || len(firstHash) != 32 {
		t.Fatal("invalid token generation")
	}
}
