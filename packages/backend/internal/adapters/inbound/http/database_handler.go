package httphandler

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
)

type DatabaseHandler struct {
	svc inbound.SourceDatabaseService
}

func NewDatabaseHandler(svc inbound.SourceDatabaseService) *DatabaseHandler {
	return &DatabaseHandler{svc: svc}
}

func (h *DatabaseHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /databases", h.listDatabases)
	mux.HandleFunc("POST /databases", h.createDatabase)
	mux.HandleFunc("GET /databases/{id}", h.getDatabase)
	mux.HandleFunc("PUT /databases/{id}", h.updateDatabase)
	mux.HandleFunc("DELETE /databases/{id}", h.deleteDatabase)
	mux.HandleFunc("POST /databases/{id}/test", h.testConnection)
}

type databaseRequest struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DBName   string `json:"dbname"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"sslmode"`
}

func (r databaseRequest) toInput() inbound.SourceDatabaseInput {
	return inbound.SourceDatabaseInput{
		Name:     r.Name,
		Host:     r.Host,
		Port:     r.Port,
		DBName:   r.DBName,
		Username: r.Username,
		Password: r.Password,
		SSLMode:  r.SSLMode,
	}
}

func (h *DatabaseHandler) listDatabases(w http.ResponseWriter, r *http.Request) {
	userID, ok := UserIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}

	dbs, err := h.svc.ListDatabases(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	responses := make([]domain.SourceDatabaseResponse, len(dbs))
	for i, db := range dbs {
		responses[i] = db.ToResponse()
	}
	writeJSON(w, http.StatusOK, responses)
}

func (h *DatabaseHandler) createDatabase(w http.ResponseWriter, r *http.Request) {
	userID, ok := UserIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return
	}

	var body databaseRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	db, err := h.svc.CreateDatabase(r.Context(), userID, body.toInput())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, db.ToResponse())
}

func (h *DatabaseHandler) getDatabase(w http.ResponseWriter, r *http.Request) {
	userID, id, ok := h.ids(w, r)
	if !ok {
		return
	}

	db, err := h.svc.GetDatabase(r.Context(), userID, id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, db.ToResponse())
}

func (h *DatabaseHandler) updateDatabase(w http.ResponseWriter, r *http.Request) {
	userID, id, ok := h.ids(w, r)
	if !ok {
		return
	}

	var body databaseRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	db, err := h.svc.UpdateDatabase(r.Context(), userID, id, body.toInput())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, db.ToResponse())
}

func (h *DatabaseHandler) deleteDatabase(w http.ResponseWriter, r *http.Request) {
	userID, id, ok := h.ids(w, r)
	if !ok {
		return
	}

	if err := h.svc.DeleteDatabase(r.Context(), userID, id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *DatabaseHandler) testConnection(w http.ResponseWriter, r *http.Request) {
	userID, id, ok := h.ids(w, r)
	if !ok {
		return
	}

	result := h.svc.TestConnection(r.Context(), userID, id)
	writeJSON(w, http.StatusOK, result)
}

func (h *DatabaseHandler) ids(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, bool) {
	userID, ok := UserIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing authenticated user")
		return uuid.Nil, uuid.Nil, false
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid database id")
		return uuid.Nil, uuid.Nil, false
	}
	return userID, id, true
}
