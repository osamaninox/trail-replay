package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/osamakhalid/trail-replay/internal/core/trail/domain"
	"github.com/osamakhalid/trail-replay/internal/core/trail/ports/outbound"
)

type postgresRepository struct {
	db              *sqlx.DB
	trailConverter  EntityConverter[*TrailEntity, *domain.Trail]
	eventConverter  EntityConverter[*EventEntity, domain.Event]
}

func NewPostgresRepository(db *sqlx.DB) outbound.TrailRepository {
	return &postgresRepository{
		db:              db,
		trailConverter:  EntityConverter[*TrailEntity, *domain.Trail]{},
		eventConverter:  EntityConverter[*EventEntity, domain.Event]{},
	}
}

func (r *postgresRepository) Save(ctx context.Context, trail *domain.Trail) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert trail
	trailEntity := TrailToEntity(trail)
	const insertTrailQuery = `
		INSERT INTO trails (id, name, created_at, updated_at)
		VALUES (:id, :name, :created_at, :updated_at)`

	_, err = tx.NamedExecContext(ctx, insertTrailQuery, trailEntity)
	if err != nil {
		return fmt.Errorf("failed to insert trail: %w", err)
	}

	// Insert events if any
	if len(trail.Events) > 0 {
		const insertEventQuery = `
			INSERT INTO events (id, trail_id, type, payload, occured_at, sequence)
			VALUES (:id, :trail_id, :type, :payload, :occured_at, :sequence)`

		for _, event := range trail.Events {
			eventEntity := EventToEntity(&event)
			_, err = tx.NamedExecContext(ctx, insertEventQuery, eventEntity)
			if err != nil {
				return fmt.Errorf("failed to insert event %s: %w", event.ID, err)
			}
		}
	}

	return tx.Commit()
}

func (r *postgresRepository) FindByID(ctx context.Context, id string) (*domain.Trail, error) {
	// Get trail
	var trailEntity TrailEntity
	const selectTrailQuery = `
		SELECT id, name, created_at, updated_at
		FROM trails
		WHERE id = $1`

	err := r.db.GetContext(ctx, &trailEntity, selectTrailQuery, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("trail %s not found", id)
		}
		return nil, fmt.Errorf("failed to get trail: %w", err)
	}

	// Get events
	var eventEntities []EventEntity
	const selectEventsQuery = `
		SELECT id, trail_id, type, payload, occured_at, sequence
		FROM events
		WHERE trail_id = $1
		ORDER BY sequence ASC`

	err = r.db.SelectContext(ctx, &eventEntities, selectEventsQuery, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get events: %w", err)
	}

	// Convert to domain
	trail := trailEntity.ToDomain()
	events := make([]domain.Event, len(eventEntities))
	for i, eventEntity := range eventEntities {
		events[i] = eventEntity.ToDomain()
	}
	trail.Events = events

	return trail, nil
}

func (r *postgresRepository) FindAll(ctx context.Context) ([]domain.Trail, error) {
	// Get all trails
	var trailEntities []TrailEntity
	const selectTrailsQuery = `
		SELECT id, name, created_at, updated_at
		FROM trails
		ORDER BY created_at DESC`

	err := r.db.SelectContext(ctx, &trailEntities, selectTrailsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get trails: %w", err)
	}

	trails := make([]domain.Trail, len(trailEntities))
	for i, trailEntity := range trailEntities {
		trail := trailEntity.ToDomain()

		// Get events for this trail
		var eventEntities []EventEntity
		const selectEventsQuery = `
			SELECT id, trail_id, type, payload, occured_at, sequence
			FROM events
			WHERE trail_id = $1
			ORDER BY sequence ASC`

		err = r.db.SelectContext(ctx, &eventEntities, selectEventsQuery, trail.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get events for trail %s: %w", trail.ID, err)
		}

		events := make([]domain.Event, len(eventEntities))
		for j, eventEntity := range eventEntities {
			events[j] = eventEntity.ToDomain()
		}
		trail.Events = events
		trails[i] = *trail
	}

	return trails, nil
}

func (r *postgresRepository) Update(ctx context.Context, trail *domain.Trail) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update trail
	trailEntity := TrailToEntity(trail)
	const updateTrailQuery = `
		UPDATE trails
		SET name = :name, updated_at = :updated_at
		WHERE id = :id`

	result, err := tx.NamedExecContext(ctx, updateTrailQuery, trailEntity)
	if err != nil {
		return fmt.Errorf("failed to update trail: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("trail %s not found", trail.ID)
	}

	// Delete existing events
	const deleteEventsQuery = `DELETE FROM events WHERE trail_id = $1`
	_, err = tx.ExecContext(ctx, deleteEventsQuery, trail.ID)
	if err != nil {
		return fmt.Errorf("failed to delete existing events: %w", err)
	}

	// Insert new events
	if len(trail.Events) > 0 {
		const insertEventQuery = `
			INSERT INTO events (id, trail_id, type, payload, occured_at, sequence)
			VALUES (:id, :trail_id, :type, :payload, :occured_at, :sequence)`

		for _, event := range trail.Events {
			eventEntity := EventToEntity(&event)
			_, err = tx.NamedExecContext(ctx, insertEventQuery, eventEntity)
			if err != nil {
				return fmt.Errorf("failed to insert event %s: %w", event.ID, err)
			}
		}
	}

	return tx.Commit()
}