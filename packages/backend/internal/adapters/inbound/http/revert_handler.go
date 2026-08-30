package httphandler

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"trail-replay/internal/core/trail/domain"
	"trail-replay/internal/core/trail/ports/inbound"
)

type RevertHandler struct {
	svc inbound.RevertService
}

func NewRevertHandler(svc inbound.RevertService) *RevertHandler {
	return &RevertHandler{svc: svc}
}

func (h *RevertHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /reverts/jobs", h.createRevertJob)
	mux.HandleFunc("GET /reverts/jobs/{id}", h.getRevertJob)
	mux.HandleFunc("GET /reverts/jobs", h.listRevertJobs)
	mux.HandleFunc("DELETE /reverts/jobs/{id}", h.cancelRevertJob)
}

func (h *RevertHandler) createRevertJob(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateRevertJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.From.IsZero() || req.To.IsZero() {
		writeError(w, http.StatusBadRequest, "from and to are required")
		return
	}
	if !req.From.Before(req.To) {
		writeError(w, http.StatusBadRequest, "from must be before to")
		return
	}

	job, err := h.svc.CreateRevertJob(r.Context(), req.From, req.To)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, job.ToResponse())
}

func (h *RevertHandler) getRevertJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.svc.GetRevertJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, job.ToResponse())
}

func (h *RevertHandler) listRevertJobs(w http.ResponseWriter, r *http.Request) {
	page := queryParamInt(r, "page", 1)
	pageSize := queryParamInt(r, "page_size", 20)

	result, err := h.svc.ListRevertJobs(r.Context(), page, pageSize)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	responses := make([]domain.RevertJobResponse, len(result.Data))
	for i, j := range result.Data {
		responses[i] = j.ToResponse()
	}

	writeJSON(w, http.StatusOK, domain.PaginatedResponse[domain.RevertJobResponse]{
		Data:       responses,
		Page:       result.Page,
		PageSize:   result.PageSize,
		TotalCount: result.TotalCount,
		TotalPages: result.TotalPages,
	})
}

func (h *RevertHandler) cancelRevertJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.svc.CancelRevertJob(r.Context(), id)
	if err != nil {
		if err.Error() == "revert job not found" {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, job.ToResponse())
}
