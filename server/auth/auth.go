package auth

type AuthService interface {
	// Returns true if the given token is valid, false otherwise
	AuthenitcateToken(token string) bool
}

type DummyAuthService struct {
	validTokens map[string]struct{}
}

func (d *DummyAuthService) AuthenitcateToken(token string) bool {
	if _, ok := d.validTokens[token]; ok {
		return true
	}

	return false
}

func NewDummyAuthService(validTokens []string) AuthService {
	tokens := map[string]struct{}{}
	for _, token := range validTokens {
		tokens[token] = struct{}{}
	}

	return &DummyAuthService{
		validTokens: tokens,
	}
}
