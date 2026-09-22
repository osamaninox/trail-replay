package httphandler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
	"trail-replay/internal/core/trail/services"
)

type AuthHandler struct {
	svc inbound.AuthService
}

func NewAuthHandler(svc inbound.AuthService) *AuthHandler {
	return &AuthHandler{svc: svc}
}

func (h *AuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/signup", h.signup)
	mux.HandleFunc("POST /auth/login", h.login)
}

func (h *AuthHandler) signup(w http.ResponseWriter, r *http.Request) {
	h.handleAuth(w, r, h.svc.Signup)
}

func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	h.handleAuth(w, r, h.svc.Login)
}

func (h *AuthHandler) handleAuth(w http.ResponseWriter, r *http.Request, fn func(ctx context.Context, email, password string) (*domain.AuthResult, error)) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	result, err := fn(r.Context(), body.Email, body.Password)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrEmailTaken):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, services.ErrInvalidCredentials):
			writeError(w, http.StatusUnauthorized, err.Error())
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, result)
}
