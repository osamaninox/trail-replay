package httphandler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"trail-replay/internal/core/trail/ports/inbound"
)

type Handler struct {
	walSvc inbound.WalQueryService
}

func NewHandler(walSvc inbound.WalQueryService) *Handler {
	return &Handler{walSvc: walSvc}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /wal/transactions", h.listWalTransactions)
}

func (h *Handler) listWalTransactions(w http.ResponseWriter, r *http.Request) {
	page := queryParamInt(r, "page", 1)
	pageSize := queryParamInt(r, "page_size", 20)

	result, err := h.walSvc.ListWalTransactions(r.Context(), page, pageSize)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func queryParamInt(r *http.Request, key string, defaultVal int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
