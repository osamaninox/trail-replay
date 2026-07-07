package services_test

import (
	"context"
	"testing"

	"github.com/osamakhalid/trail-replay/internal/adapters/outbound/storage"
	"github.com/osamakhalid/trail-replay/internal/core/domain"
	"github.com/osamakhalid/trail-replay/internal/core/ports/inbound"
	"github.com/osamakhalid/trail-replay/internal/core/services"
)

func newService() inbound.TrailService {
	return services.NewTrailService(storage.NewInMemoryRepository())
}

func TestCreateAndGetTrail(t *testing.T) {
	ctx := context.Background()
	svc := newService()

	trail, err := svc.CreateTrail(ctx, "my-trail")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if trail.Name != "my-trail" {
		t.Errorf("name mismatch: got %q", trail.Name)
	}

	got, err := svc.GetTrail(ctx, trail.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != trail.ID {
		t.Errorf("id mismatch")
	}
}

func TestAppendAndReplay(t *testing.T) {
	ctx := context.Background()
	svc := newService()

	trail, _ := svc.CreateTrail(ctx, "replay-trail")

	for _, et := range []domain.EventType{domain.EventTypeCreated, domain.EventTypeUpdated, domain.EventTypeDeleted} {
		_, err := svc.AppendEvent(ctx, trail.ID, et, map[string]any{"key": "val"})
		if err != nil {
			t.Fatalf("append %s: %v", et, err)
		}
	}

	events, err := svc.ReplayTrail(ctx, trail.ID, 2)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events from seq 2, got %d", len(events))
	}
}
