package httphandler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
	"github.com/osamakhalid/trail-replay/internal/core/trail/ports/inbound"
)

type Handler struct {
	svc inbound.TrailService
}

func NewHandler(svc inbound.TrailService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /trails", h.createTrail)
	mux.HandleFunc("GET /trails", h.listTrails)
	mux.HandleFunc("GET /trails/{id}", h.getTrail)
	mux.HandleFunc("POST /trails/{id}/events", h.appendEvent)
	mux.HandleFunc("GET /trails/{id}/replay", h.replayTrail)
}

func (h *Handler) createTrail(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	trail, err := h.svc.CreateTrail(r.Context(), body.Name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, trail)
}

func (h *Handler) listTrails(w http.ResponseWriter, r *http.Request) {
	trails, err := h.svc.ListTrails(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, trails)
}

func (h *Handler) getTrail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	trail, err := h.svc.GetTrail(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, trail)
}

func (h *Handler) appendEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Type    domain.EventType `json:"type"`
		Payload map[string]any   `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	event, err := h.svc.AppendEvent(r.Context(), id, body.Type, body.Payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, event)
}

func (h *Handler) replayTrail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fromStr := r.URL.Query().Get("from")
	var from int64 = 1
	if fromStr != "" {
		parsed, err := strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'from' query parameter")
			return
		}
		from = parsed
	}
	events, err := h.svc.ReplayTrail(r.Context(), id, from)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
