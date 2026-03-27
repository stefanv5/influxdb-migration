package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/migration-tools/influx-migrator/pkg/types"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dir string) (*SQLiteStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint dir: %w", err)
	}

	dbPath := filepath.Join(dir, "checkpoints.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping sqlite: %w", err)
	}

	store := &SQLiteStore{db: db}
	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to init schema: %w", err)
	}

	return store, nil
}

func (s *SQLiteStore) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS checkpoints (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		task_name TEXT NOT NULL,
		source_table TEXT NOT NULL,
		target_meas TEXT NOT NULL,
		last_id INTEGER DEFAULT 0,
		last_timestamp TEXT,
		processed_rows INTEGER DEFAULT 0,
		status TEXT DEFAULT 'pending',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		error_message TEXT,
		mapping_config TEXT,
		UNIQUE(task_id, source_table)
	);

	CREATE TABLE IF NOT EXISTS migration_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT UNIQUE NOT NULL,
		task_name TEXT NOT NULL,
		source_adapter TEXT NOT NULL,
		target_adapter TEXT NOT NULL,
		status TEXT DEFAULT 'pending',
		total_rows INTEGER DEFAULT 0,
		migrated_rows INTEGER DEFAULT 0,
		failed_rows INTEGER DEFAULT 0,
		started_at TEXT,
		completed_at TEXT,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS schema_version (
		version INTEGER PRIMARY KEY
	);
	`

	_, err := s.db.Exec(schema)

	return err
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) SaveCheckpoint(cp *types.Checkpoint) error {
	query := `
	INSERT INTO checkpoints (task_id, task_name, source_table, target_meas, last_id, last_timestamp, processed_rows, status, created_at, updated_at, error_message, mapping_config)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(task_id, source_table) DO UPDATE SET
		last_id = excluded.last_id,
		last_timestamp = excluded.last_timestamp,
		processed_rows = excluded.processed_rows,
		status = excluded.status,
		updated_at = excluded.updated_at,
		error_message = excluded.error_message,
		mapping_config = excluded.mapping_config
	`

	now := time.Now().UTC()
	ts := ""
	if cp.LastTimestamp != 0 {
		ts = time.Unix(0, cp.LastTimestamp).Format(time.RFC3339Nano)
	}

	var mappingConfigJSON []byte
	if cp.MappingConfig.SourceTable != "" {
		var err error
		mappingConfigJSON, err = json.Marshal(cp.MappingConfig)
		if err != nil {
			return fmt.Errorf("failed to marshal mapping config: %w", err)
		}
	}

	_, err := s.db.Exec(query,
		cp.TaskID, cp.TaskName, cp.SourceTable, cp.TargetMeas,
		cp.LastID, ts, cp.ProcessedRows, cp.Status,
		now.Format(time.RFC3339), now.Format(time.RFC3339),
		cp.ErrorMessage, string(mappingConfigJSON),
	)
	return err
}

func (s *SQLiteStore) LoadCheckpoint(taskID, sourceTable string) (*types.Checkpoint, error) {
	query := `SELECT id, task_id, task_name, source_table, target_meas, last_id, last_timestamp, processed_rows, status, created_at, updated_at, error_message, mapping_config
	          FROM checkpoints WHERE task_id = ? AND source_table = ?`

	var cp types.Checkpoint
	var lastTS, createdAt, updatedAt, mappingConfigJSON sql.NullString

	err := s.db.QueryRow(query, taskID, sourceTable).Scan(
		&cp.ID, &cp.TaskID, &cp.TaskName, &cp.SourceTable, &cp.TargetMeas,
		&cp.LastID, &lastTS, &cp.ProcessedRows, &cp.Status,
		&createdAt, &updatedAt, &cp.ErrorMessage, &mappingConfigJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if lastTS.Valid {
		if t, err := time.Parse(time.RFC3339Nano, lastTS.String); err == nil {
			cp.LastTimestamp = t.UnixNano()
		} else if t, err := time.Parse(time.RFC3339, lastTS.String); err == nil {
			cp.LastTimestamp = t.UnixNano()
		}
	}

	if mappingConfigJSON.Valid && mappingConfigJSON.String != "" {
		if err := json.Unmarshal([]byte(mappingConfigJSON.String), &cp.MappingConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal mapping config: %w", err)
		}
	}

	return &cp, nil
}

func (s *SQLiteStore) ListCheckpoints(taskID string) ([]*types.Checkpoint, error) {
	query := `SELECT id, task_id, task_name, source_table, target_meas, last_id, last_timestamp, processed_rows, status, created_at, updated_at, error_message, mapping_config
	          FROM checkpoints WHERE task_id = ?`

	rows, err := s.db.Query(query, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checkpoints []*types.Checkpoint
	for rows.Next() {
		var cp types.Checkpoint
		var lastTS, createdAt, updatedAt, mappingConfigJSON sql.NullString

		err := rows.Scan(
			&cp.ID, &cp.TaskID, &cp.TaskName, &cp.SourceTable, &cp.TargetMeas,
			&cp.LastID, &lastTS, &cp.ProcessedRows, &cp.Status,
			&createdAt, &updatedAt, &cp.ErrorMessage, &mappingConfigJSON,
		)
		if err != nil {
			return nil, err
		}

		if lastTS.Valid {
			if t, err := time.Parse(time.RFC3339Nano, lastTS.String); err == nil {
				cp.LastTimestamp = t.UnixNano()
			} else if t, err := time.Parse(time.RFC3339, lastTS.String); err == nil {
				cp.LastTimestamp = t.UnixNano()
			}
		}

		if mappingConfigJSON.Valid && mappingConfigJSON.String != "" {
			if err := json.Unmarshal([]byte(mappingConfigJSON.String), &cp.MappingConfig); err != nil {
				return nil, err
			}
		}

		checkpoints = append(checkpoints, &cp)
	}

	return checkpoints, rows.Err()
}

func (s *SQLiteStore) GetTasksByStatus(status types.CheckpointStatus) ([]*types.Checkpoint, error) {
	query := `SELECT id, task_id, task_name, source_table, target_meas, last_id, last_timestamp, processed_rows, status, created_at, updated_at, error_message, mapping_config
	          FROM checkpoints WHERE status = ?`

	rows, err := s.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checkpoints []*types.Checkpoint
	for rows.Next() {
		var cp types.Checkpoint
		var lastTS, createdAt, updatedAt, mappingConfigJSON sql.NullString

		err := rows.Scan(
			&cp.ID, &cp.TaskID, &cp.TaskName, &cp.SourceTable, &cp.TargetMeas,
			&cp.LastID, &lastTS, &cp.ProcessedRows, &cp.Status,
			&createdAt, &updatedAt, &cp.ErrorMessage, &mappingConfigJSON,
		)
		if err != nil {
			return nil, err
		}

		if lastTS.Valid {
			if t, err := time.Parse(time.RFC3339Nano, lastTS.String); err == nil {
				cp.LastTimestamp = t.UnixNano()
			} else if t, err := time.Parse(time.RFC3339, lastTS.String); err == nil {
				cp.LastTimestamp = t.UnixNano()
			}
		}

		if mappingConfigJSON.Valid && mappingConfigJSON.String != "" {
			if err := json.Unmarshal([]byte(mappingConfigJSON.String), &cp.MappingConfig); err != nil {
				return nil, err
			}
		}

		checkpoints = append(checkpoints, &cp)
	}

	return checkpoints, rows.Err()
}

func (s *SQLiteStore) ResetAll() error {
	query := `DELETE FROM checkpoints`
	_, err := s.db.Exec(query)
	return err
}

func (s *SQLiteStore) DeleteCheckpoint(taskID, sourceTable string) error {
	query := `DELETE FROM checkpoints WHERE task_id = ? AND source_table = ?`
	_, err := s.db.Exec(query, taskID, sourceTable)
	return err
}
