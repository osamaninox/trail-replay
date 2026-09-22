package httphandler

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/ports/inbound"
)

type contextKey string

const userIDKey contextKey = "userID"

// UserIDFromContext extracts the authenticated user's ID injected by AuthMiddleware.
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(userIDKey).(uuid.UUID)
	return id, ok
}

// AuthMiddleware validates the JWT bearer token on protected routes and injects
// the user ID into the request context. /auth/* routes and CORS preflight
// requests are left unauthenticated.
func AuthMiddleware(authSvc inbound.AuthService, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || strings.HasPrefix(r.URL.Path, "/auth/") {
			next.ServeHTTP(w, r)
			return
		}

		header := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(header, "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "missing or malformed authorization header")
			return
		}

		userID, err := authSvc.ValidateToken(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDKey, userID)))
	})
}

// CORSMiddleware allows cross-origin requests from the frontend dev server /
// deployed origin.
func CORSMiddleware(allowedOrigin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
