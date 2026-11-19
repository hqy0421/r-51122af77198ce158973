// Copyright [2026] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// Tx operation symbols used by the TxOp Avro enum.
const (
	TxOpInsert = "I"
	TxOpUpdate = "U"
	TxOpDelete = "D"
)

// Bookkeeping table and metadata key names.  All goavro bookkeeping is kept
// separate from the application's target tables so that receive progress can
// never masquerade as applied (durable) progress.
const (
	metaTable      = "_goavro_tx_meta"
	fragmentTable  = "_goavro_tx_fragment"
	commitTable    = "_goavro_tx_commit"
	metaKeyApplied = "applied_head"
	metaKeyReceive = "receive_cursor"
)

// ErrBlocked is returned when the head transaction cannot be applied because
// a SQL statement or a deferred foreign-key constraint failed.  Already
// applied transactions stay committed; the failed transaction and every
// later transaction remain persisted but unapplied, blocking the head until
// the underlying problem is fixed and Pump is retried.
type ErrBlocked struct {
	TxID      int64
	CommitSeq int64
	Err       error
}

func (e *ErrBlocked) Error() string {
	return fmt.Sprintf("transaction %d (commitSeq %d) blocked: %s", e.TxID, e.CommitSeq, e.Err)
}

func (e *ErrBlocked) Unwrap() error { return e.Err }

// ErrConflict is returned when a retransmitted fragment contradicts an
// already persisted fragment: the same (txID, slot) or the same txID commit
// carries different content.  Re-sending identical content is accepted and
// stays idempotent.
type ErrConflict struct {
	TxID int64
	Slot int32 // -1 for commit fragments
	Msg  string
}

func (e *ErrConflict) Error() string {
	if e.Slot < 0 {
		return fmt.Sprintf("conflicting commit fragment for txID %d: %s", e.TxID, e.Msg)
	}
	return fmt.Sprintf("conflicting data fragment for txID %d slot %d: %s", e.TxID, e.Slot, e.Msg)
}

// TableBinding describes how decoded row maps of one target table are turned
// into SQL.  Columns is the ordered list of column names every I/U row map
// must contain (D rows only need the primary-key columns).  PrimaryKey is the
// ordered list of columns used for UPDATE and DELETE.  Table and column
// identifiers are quoted as SQLite identifiers; they must be simple names.
type TableBinding struct {
	Table      string
	Columns    []string
	PrimaryKey []string
}

// FragmentLoaderConfig configures a FragmentLoader.
type FragmentLoaderConfig struct {
	// DSN of the SQLite database.  Both target tables and goavro bookkeeping
	// tables live here.  A typical value is "file:loader.db".
	DSN string
	// Registry maps wire schema IDs to Avro Codecs.  Required.
	Registry *SchemaRegistry
	// Bindings maps the "table" field of TxData fragments to SQL bindings.
	// Every referenced table must be present.
	Bindings map[string]TableBinding
}

// IngestResult reports what an Ingest call did.
type IngestResult struct {
	// Kind is "DATA" or "COMMIT".
	Kind string
	TxID int64
	// Slot is the data fragment slot (-1 for commit fragments).
	Slot int32
	// Duplicate is true when an identical fragment was already persisted.
	Duplicate bool
	// ReceivePosition is the persisted receive cursor after this ingest.
	ReceivePosition int64
	// AppliedHead is the highest commit sequence durably applied to the
	// target tables after this ingest.
	AppliedHead int64
	// Applied is the list of commit sequence numbers applied by the Pump
	// triggered at the end of this ingest, in order.
	Applied []int64
	// PumpError is non-nil when the ingest itself succeeded but ordered
	// application of the head transaction failed (ErrBlocked).  The
	// fragment remains durably persisted; retry via Pump after fixing the
	// underlying condition.
	PumpError error
}

// FragmentLoader ingests schema-ID-prefixed Avro transaction fragments,
// persists them before any side effect, and applies complete transactions to
// SQLite target tables strictly in commit order.
//
// Receive progress (how many messages were accepted) and applied progress
// (which commit sequence is durable in the target tables) are tracked
// independently.  A FragmentLoader is safe for concurrent use; all public
// methods are serialized internally.
type FragmentLoader struct {
	db       *sql.DB
	registry *SchemaRegistry
	bindings map[string]TableBinding

	mu sync.Mutex
}

// NewFragmentLoader opens (or creates) the SQLite database, ensures the
// goavro bookkeeping tables exist, and recovers the persisted applied head so
// that previously accepted but not yet applied fragments survive a reopen.
func NewFragmentLoader(cfg FragmentLoaderConfig) (*FragmentLoader, error) {
	if cfg.Registry == nil {
		return nil, errors.New("FragmentLoaderConfig.Registry is required")
	}
	if cfg.DSN == "" {
		return nil, errors.New("FragmentLoaderConfig.DSN is required")
	}
	db, err := sql.Open("sqlite3", cfg.DSN)
	if err != nil {
		return nil, err
	}
	// All bookkeeping relies on transaction ordering; a single connection
	// also gives every transaction a stable PRAGMA defer_foreign_keys scope.
	db.SetMaxOpenConns(1)

	l := &FragmentLoader{
		db:       db,
		registry: cfg.Registry,
		bindings: make(map[string]TableBinding, len(cfg.Bindings)),
	}
	for name, b := range cfg.Bindings {
		if b.Table == "" {
			db.Close()
			return nil, fmt.Errorf("binding %q: empty table name", name)
		}
		if len(b.Columns) == 0 {
			db.Close()
			return nil, fmt.Errorf("binding %q: Columns must not be empty", name)
		}
		if len(b.PrimaryKey) == 0 {
			db.Close()
			return nil, fmt.Errorf("binding %q: PrimaryKey must not be empty", name)
		}
		for _, pk := range b.PrimaryKey {
			if !containsString(b.Columns, pk) {
				db.Close()
				return nil, fmt.Errorf("binding %q: primary key column %q is not in Columns", name, pk)
			}
		}
		l.bindings[name] = b
	}

	if err := l.init(); err != nil {
		db.Close()
		return nil, err
	}
	return l, nil
}

// Close releases the underlying database handle.
func (l *FragmentLoader) Close() error { return l.db.Close() }

func (l *FragmentLoader) init() error {
	if _, err := l.db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("cannot set WAL journal mode: %w", err)
	}
	if _, err := l.db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("cannot enable foreign keys: %w", err)
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + metaTable + ` (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ` + fragmentTable + ` (
			tx_id       INTEGER NOT NULL,
			slot        INTEGER NOT NULL,
			table_name  TEXT NOT NULL,
			op          TEXT NOT NULL,
			payload     TEXT NOT NULL,
			applied     INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (tx_id, slot)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + commitTable + ` (
			tx_id      INTEGER PRIMARY KEY,
			commit_seq INTEGER NOT NULL UNIQUE,
			slot_count INTEGER NOT NULL,
			payload    TEXT NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := l.db.Exec(s); err != nil {
			return fmt.Errorf("cannot initialize bookkeeping tables: %w", err)
		}
	}
	if err := l.initMeta(metaKeyApplied, "0"); err != nil {
		return err
	}
	return l.initMeta(metaKeyReceive, "0")
}

func (l *FragmentLoader) initMeta(key, defaultVal string) error {
	_, err := l.db.Exec(
		`INSERT OR IGNORE INTO `+metaTable+` (key, value) VALUES (?, ?)`,
		key, defaultVal)
	if err != nil {
		return fmt.Errorf("cannot initialize metadata %q: %w", key, err)
	}
	return nil
}

// AppliedHead returns the highest commit sequence number durably applied to
// the target tables.  It only advances inside the same SQLite transaction
// that commits the target table changes, so it can never overtake reality.
func (l *FragmentLoader) AppliedHead() (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readMetaInt(metaKeyApplied)
}

// ReceivePosition returns how many messages have been accepted and persisted.
// It is deliberately separate from AppliedHead.
func (l *FragmentLoader) ReceivePosition() (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readMetaInt(metaKeyReceive)
}

func (l *FragmentLoader) readMetaInt(key string) (int64, error) {
	var v string
	row := l.db.QueryRow(`SELECT value FROM `+metaTable+` WHERE key = ?`, key)
	if err := row.Scan(&v); err != nil {
		return 0, fmt.Errorf("cannot read metadata %q: %w", key, err)
	}
	var n int64
	if _, err := fmt.Sscan(v, &n); err != nil {
		return 0, fmt.Errorf("cannot parse metadata %q value %q: %w", key, v, err)
	}
	return n, nil
}

// Ingest decodes a schema-ID-prefixed Avro wire message, persists the
// fragment and the advanced receive cursor first, then applies every
// contiguous fully-slotted transaction whose turn has come.  The receive
// cursor auto-advances by one per accepted message.
func (l *FragmentLoader) Ingest(wire []byte) (*IngestResult, error) {
	return l.IngestAt(0, wire)
}

// IngestAt is Ingest with an explicit receive position.  position must be
// strictly greater than the previously accepted position; 0 means
// auto-advance.
func (l *FragmentLoader) IngestAt(position int64, wire []byte) (*IngestResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	datum, _, err := l.registry.DecodeWire(wire)
	if err != nil {
		return nil, fmt.Errorf("cannot decode wire message: %w", err)
	}
	record, ok := datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded wire payload is %T, expected record map", datum)
	}
	kind, err := recordString(record, "kind")
	if err != nil {
		return nil, err
	}
	txID, err := recordLong(record, "txID")
	if err != nil {
		return nil, err
	}

	res := &IngestResult{Kind: kind, TxID: txID, Slot: -1}

	tx, err := l.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("cannot begin ingest transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// The receive cursor advances in the SAME transaction as the fragment
	// insert: a persisted fragment always has an advanced cursor, and a
	// rolled-back ingest advances nothing.
	prevPos, err := l.metaIntTx(tx, metaKeyReceive)
	if err != nil {
		return nil, err
	}
	nextPos := position
	if nextPos == 0 {
		nextPos = prevPos + 1
	} else if nextPos <= prevPos {
		return nil, fmt.Errorf("receive position %d is not greater than persisted cursor %d", nextPos, prevPos)
	}

	var canonical string
	switch kind {
	case "DATA":
		slot, tableName, op, rowMap, perr := l.parseData(record)
		if perr != nil {
			return nil, perr
		}
		res.Slot = slot
		canonical, err = canonicalPayload(slot, tableName, op, rowMap)
		if err != nil {
			return nil, err
		}
		// Idempotency / conflict detection against anything already
		// persisted for this (txID, slot).
		var existing string
		qrow := tx.QueryRow(`SELECT payload FROM `+fragmentTable+` WHERE tx_id = ? AND slot = ?`, txID, slot)
		switch scanErr := qrow.Scan(&existing); scanErr {
		case nil:
			if existing != canonical {
				return nil, &ErrConflict{TxID: txID, Slot: slot, Msg: "different row content for the same slot"}
			}
			res.Duplicate = true
		case sql.ErrNoRows:
			if err := l.validateData(txID, slot, tableName, op, rowMap); err != nil {
				return nil, err
			}
			if err := l.checkAgainstCommit(tx, txID, slot); err != nil {
				return nil, err
			}
			if _, err = tx.Exec(
				`INSERT INTO `+fragmentTable+` (tx_id, slot, table_name, op, payload, applied)
				 VALUES (?, ?, ?, ?, ?, 0)`,
				txID, slot, tableName, op, canonical); err != nil {
				return nil, fmt.Errorf("cannot persist data fragment: %w", err)
			}
		default:
			return nil, fmt.Errorf("cannot look up existing fragment: %w", scanErr)
		}
	case "COMMIT":
		commitSeq, slotCount, perr := parseCommit(record)
		if perr != nil {
			return nil, perr
		}
		canonical, err = canonicalCommit(commitSeq, slotCount)
		if err != nil {
			return nil, err
		}
		var existing string
		qrow := tx.QueryRow(`SELECT payload FROM `+commitTable+` WHERE tx_id = ?`, txID)
		switch scanErr := qrow.Scan(&existing); scanErr {
		case nil:
			if existing != canonical {
				return nil, &ErrConflict{TxID: txID, Slot: -1, Msg: "different commitSeq/slotCount for the same txID"}
			}
			res.Duplicate = true
		case sql.ErrNoRows:
			if commitSeq <= 0 {
				return nil, fmt.Errorf("txID %d: commitSeq must be positive, got %d", txID, commitSeq)
			}
			if slotCount < 0 {
				return nil, fmt.Errorf("txID %d: slotCount must not be negative, got %d", txID, slotCount)
			}
			// Reject a reused commit sequence belonging to a different
			// transaction even before it reaches the head.
			var other int64
			dupRow := tx.QueryRow(`SELECT tx_id FROM `+commitTable+` WHERE commit_seq = ?`, commitSeq)
			switch dupErr := dupRow.Scan(&other); dupErr {
			case nil:
				if other != txID {
					return nil, &ErrConflict{
						TxID: txID, Slot: -1,
						Msg: fmt.Sprintf("commitSeq %d already belongs to txID %d", commitSeq, other),
					}
				}
			case sql.ErrNoRows:
			default:
				return nil, fmt.Errorf("cannot check commitSeq uniqueness: %w", dupErr)
			}
			if _, err = tx.Exec(
				`INSERT INTO `+commitTable+` (tx_id, commit_seq, slot_count, payload)
				 VALUES (?, ?, ?, ?)`,
				txID, commitSeq, slotCount, canonical); err != nil {
				return nil, fmt.Errorf("cannot persist commit fragment: %w", err)
			}
			// A commit arriving before all data lets us reject out-of-range
			// slots immediately.
			if err := l.checkSlotRange(tx, txID, slotCount); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("cannot look up existing commit: %w", scanErr)
		}
	default:
		return nil, fmt.Errorf("unknown fragment kind %q", kind)
	}

	if err := l.setMetaTx(tx, metaKeyReceive, fmt.Sprintf("%d", nextPos)); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("cannot commit ingest transaction: %w", err)
	}
	committed = true
	res.ReceivePosition = nextPos

	// Apply whatever contiguous, complete transactions are now ready.  A
	// blocked head does not fail the ingest: the fragment itself is durable.
	applied, head, pumpErr := l.pumpLocked()
	res.Applied = applied
	res.AppliedHead = head
	res.PumpError = pumpErr
	return res, nil
}

// Pump applies all contiguous fully-slotted transactions starting at
// appliedHead+1.  It is invoked automatically after every ingest and may be
// called manually after a reopen or after fixing the condition behind an
// ErrBlocked.
func (l *FragmentLoader) Pump() ([]int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	applied, _, err := l.pumpLocked()
	return applied, err
}

func (l *FragmentLoader) pumpLocked() ([]int64, int64, error) {
	var applied []int64
	for {
		head, err := l.readMetaInt(metaKeyApplied)
		if err != nil {
			return applied, head, err
		}
		nextSeq := head + 1

		var txID int64
		var slotCount int32
		row := l.db.QueryRow(
			`SELECT tx_id, slot_count FROM `+commitTable+` WHERE commit_seq = ?`, nextSeq)
		switch err := row.Scan(&txID, &slotCount); err {
		case nil:
		case sql.ErrNoRows:
			return applied, head, nil // next transaction not fully known yet
		default:
			return applied, head, fmt.Errorf("cannot look up head transaction: %w", err)
		}

		var got int
		countRow := l.db.QueryRow(
			`SELECT COUNT(*) FROM `+fragmentTable+` WHERE tx_id = ?`, txID)
		if err := countRow.Scan(&got); err != nil {
			return applied, head, fmt.Errorf("cannot count fragments for txID %d: %w", txID, err)
		}
		if int32(got) < slotCount {
			// Missing slots: wait for the rest of the transaction.
			return applied, head, nil
		}
		if int32(got) > slotCount {
			// The (tx_id, slot) primary key makes duplicates impossible, so
			// this means slots are not exactly the announced set.
			return applied, head, &ErrBlocked{TxID: txID, CommitSeq: nextSeq,
				Err: fmt.Errorf("found %d fragments but commit announced slotCount %d", got, slotCount)}
		}
		// Same count: applyOne additionally verifies the slots are the
		// contiguous 0..slotCount-1 sequence.

		if err := l.applyOne(txID, nextSeq, slotCount); err != nil {
			return applied, head, err
		}
		applied = append(applied, nextSeq)
	}
}

// applyOne applies a single complete transaction in one SQLite transaction:
// target table changes, fragment applied flags and appliedHead all commit or
// roll back together.  Deferred foreign keys allow child rows before parent
// rows; a final constraint failure rolls the whole transaction back.
func (l *FragmentLoader) applyOne(txID int64, commitSeq int64, slotCount int32) error {
	tx, err := l.db.Begin()
	if err != nil {
		return fmt.Errorf("cannot begin apply transaction for txID %d: %w", txID, err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	// Defer every foreign-key check until COMMIT so fragments can be
	// applied child-before-parent inside the same transaction.
	if _, err := tx.Exec(`PRAGMA defer_foreign_keys=ON`); err != nil {
		return fmt.Errorf("cannot defer foreign keys for txID %d: %w", txID, err)
	}

	rows, err := tx.Query(
		`SELECT slot, table_name, op, payload FROM `+fragmentTable+`
		 WHERE tx_id = ? ORDER BY slot ASC`, txID)
	if err != nil {
		return fmt.Errorf("cannot read fragments for txID %d: %w", txID, err)
	}
	type frag struct {
		slot      int32
		tableName string
		op        string
		payload   string
	}
	var frags []frag
	for rows.Next() {
		var f frag
		if err := rows.Scan(&f.slot, &f.tableName, &f.op, &f.payload); err != nil {
			rows.Close()
			return fmt.Errorf("cannot scan fragment for txID %d: %w", txID, err)
		}
		frags = append(frags, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("fragment iteration failed for txID %d: %w", txID, err)
	}
	if len(frags) != int(slotCount) {
		return &ErrBlocked{TxID: txID, CommitSeq: commitSeq,
			Err: fmt.Errorf("expected %d slots, found %d", slotCount, len(frags))}
	}

	for i, f := range frags {
		if int32(i) != f.slot {
			return &ErrBlocked{TxID: txID, CommitSeq: commitSeq,
				Err: fmt.Errorf("slots must be contiguous 0..%d, found slot %d at position %d", slotCount-1, f.slot, i)}
		}
		rowMap, err := decodePayloadRow(f.payload)
		if err != nil {
			return &ErrBlocked{TxID: txID, CommitSeq: commitSeq, Err: err}
		}
		stmt, args, err := l.buildSQL(f.tableName, f.op, rowMap)
		if err != nil {
			return &ErrBlocked{TxID: txID, CommitSeq: commitSeq, Err: err}
		}
		if _, err := tx.Exec(stmt, args...); err != nil {
			return &ErrBlocked{TxID: txID, CommitSeq: commitSeq,
				Err: fmt.Errorf("slot %d table %q op %s failed: %w", f.slot, f.tableName, f.op, err)}
		}
	}

	// Mark fragments applied and advance the head in the SAME transaction
	// as the target changes.  A crash can therefore never expose an advanced
	// appliedHead without the rows, nor rows without the advanced head.
	if _, err := tx.Exec(
		`UPDATE `+fragmentTable+` SET applied = 1 WHERE tx_id = ?`, txID); err != nil {
		return fmt.Errorf("cannot mark fragments applied for txID %d: %w", txID, err)
	}
	if err := l.setMetaTx(tx, metaKeyApplied, fmt.Sprintf("%d", commitSeq)); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		// Deferred foreign-key violations and any write failure surface here.
		return &ErrBlocked{TxID: txID, CommitSeq: commitSeq,
			Err: fmt.Errorf("commit failed (constraint or write error): %w", err)}
	}
	rollback = false
	return nil
}

// buildSQL turns one decoded row map into a parameterized statement.
func (l *FragmentLoader) buildSQL(tableName, op string, row map[string]interface{}) (string, []interface{}, error) {
	binding, ok := l.bindings[tableName]
	if !ok {
		return "", nil, fmt.Errorf("no table binding registered for table %q", tableName)
	}
	switch op {
	case TxOpInsert, TxOpUpdate:
		if err := checkRowKeys(binding, row, true); err != nil {
			return "", nil, err
		}
	case TxOpDelete:
		if err := checkRowKeys(binding, row, false); err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("unknown op %q for table %q", op, tableName)
	}

	quotedTable := quoteIdent(binding.Table)

	switch op {
	case TxOpInsert:
		args, err := sqlArgs(binding.Columns, row)
		if err != nil {
			return "", nil, err
		}
		cols := make([]string, len(binding.Columns))
		placeholders := make([]string, len(binding.Columns))
		for i, c := range binding.Columns {
			cols[i] = quoteIdent(c)
			placeholders[i] = "?"
		}
		stmt := "INSERT INTO " + quotedTable + " (" + strings.Join(cols, ", ") +
			") VALUES (" + strings.Join(placeholders, ", ") + ")"
		return stmt, args, nil
	case TxOpUpdate:
		setCols := make([]string, 0, len(binding.Columns))
		setArgs := make([]interface{}, 0, len(binding.Columns)+len(binding.PrimaryKey))
		for _, c := range binding.Columns {
			if containsString(binding.PrimaryKey, c) {
				continue
			}
			setCols = append(setCols, quoteIdent(c)+" = ?")
			v, err := sqlValue(row[c])
			if err != nil {
				return "", nil, fmt.Errorf("table %q column %q: %w", tableName, c, err)
			}
			setArgs = append(setArgs, v)
		}
		whereParts := make([]string, len(binding.PrimaryKey))
		for i, pk := range binding.PrimaryKey {
			whereParts[i] = quoteIdent(pk) + " = ?"
			v, err := sqlValue(row[pk])
			if err != nil {
				return "", nil, fmt.Errorf("table %q primary key %q: %w", tableName, pk, err)
			}
			setArgs = append(setArgs, v)
		}
		stmt := "UPDATE " + quotedTable + " SET " + strings.Join(setCols, ", ") +
			" WHERE " + strings.Join(whereParts, " AND ")
		return stmt, setArgs, nil
	default: // DELETE
		whereParts := make([]string, len(binding.PrimaryKey))
		pkArgs := make([]interface{}, len(binding.PrimaryKey))
		for i, pk := range binding.PrimaryKey {
			whereParts[i] = quoteIdent(pk) + " = ?"
			v, err := sqlValue(row[pk])
			if err != nil {
				return "", nil, fmt.Errorf("table %q primary key %q: %w", tableName, pk, err)
			}
			pkArgs[i] = v
		}
		stmt := "DELETE FROM " + quotedTable + " WHERE " + strings.Join(whereParts, " AND ")
		return stmt, pkArgs, nil
	}
}

// checkRowKeys ensures a row map carries exactly the columns the binding
// expects; deletes only need the primary key.
func checkRowKeys(binding TableBinding, row map[string]interface{}, allColumns bool) error {
	required := binding.PrimaryKey
	if allColumns {
		required = binding.Columns
	}
	seen := make(map[string]bool, len(row))
	for k := range row {
		if !containsString(binding.Columns, k) {
			return fmt.Errorf("table %q: row contains unbound column %q", binding.Table, k)
		}
		seen[k] = true
	}
	for _, c := range required {
		if !seen[c] {
			return fmt.Errorf("table %q: row is missing required column %q", binding.Table, c)
		}
	}
	return nil
}

func (l *FragmentLoader) validateData(txID int64, slot int32, tableName, op string, row map[string]interface{}) error {
	if slot < 0 {
		return fmt.Errorf("txID %d: slot must be >= 0, got %d", txID, slot)
	}
	binding, ok := l.bindings[tableName]
	if !ok {
		return fmt.Errorf("txID %d slot %d: no table binding registered for table %q", txID, slot, tableName)
	}
	switch op {
	case TxOpInsert, TxOpUpdate:
		if err := checkRowKeys(binding, row, true); err != nil {
			return fmt.Errorf("txID %d slot %d: %w", txID, slot, err)
		}
	case TxOpDelete:
		if err := checkRowKeys(binding, row, false); err != nil {
			return fmt.Errorf("txID %d slot %d: %w", txID, slot, err)
		}
	default:
		return fmt.Errorf("txID %d slot %d: unknown op %q", txID, slot, op)
	}
	// Every value must be representable as a SQL parameter.
	for col, v := range row {
		if _, err := sqlValue(v); err != nil {
			return fmt.Errorf("txID %d slot %d column %q: %w", txID, slot, col, err)
		}
	}
	return nil
}

// checkAgainstCommit rejects a data fragment whose slot is outside the slot
// range announced by a commit fragment that arrived first.
func (l *FragmentLoader) checkAgainstCommit(tx *sql.Tx, txID int64, slot int32) error {
	var slotCount int32
	qrow := tx.QueryRow(`SELECT slot_count FROM `+commitTable+` WHERE tx_id = ?`, txID)
	switch err := qrow.Scan(&slotCount); err {
	case nil:
		if slot < 0 || slot >= slotCount {
			return &ErrConflict{TxID: txID, Slot: slot,
				Msg: fmt.Sprintf("slot outside commit slotCount %d", slotCount)}
		}
		return nil
	case sql.ErrNoRows:
		return nil
	default:
		return fmt.Errorf("cannot validate slot range for txID %d: %w", txID, err)
	}
}

// checkSlotRange rejects already persisted slots that fall outside the
// newly arrived commit's [0, slotCount) range.
func (l *FragmentLoader) checkSlotRange(tx *sql.Tx, txID int64, slotCount int32) error {
	rows, err := tx.Query(
		`SELECT slot FROM `+fragmentTable+` WHERE tx_id = ? AND (slot < 0 OR slot >= ?)`,
		txID, slotCount)
	if err != nil {
		return fmt.Errorf("cannot validate slot range for txID %d: %w", txID, err)
	}
	defer rows.Close()
	if rows.Next() {
		var bad int32
		_ = rows.Scan(&bad)
		return &ErrConflict{TxID: txID, Slot: bad,
			Msg: fmt.Sprintf("slot outside commit slotCount %d", slotCount)}
	}
	return rows.Err()
}

func (l *FragmentLoader) parseData(record map[string]interface{}) (int32, string, string, map[string]interface{}, error) {
	slot, err := recordInt(record, "slot")
	if err != nil {
		return 0, "", "", nil, err
	}
	tableName, err := recordString(record, "table")
	if err != nil {
		return 0, "", "", nil, err
	}
	op, err := recordString(record, "op")
	if err != nil {
		return 0, "", "", nil, err
	}
	rawRow, ok := record["row"]
	if !ok {
		return 0, "", "", nil, errors.New("TxData record missing row field")
	}
	rowMap, ok := rawRow.(map[string]interface{})
	if !ok {
		return 0, "", "", nil, fmt.Errorf("TxData row is %T, expected map", rawRow)
	}
	return slot, tableName, op, rowMap, nil
}

func parseCommit(record map[string]interface{}) (int64, int32, error) {
	commitSeq, err := recordLong(record, "commitSeq")
	if err != nil {
		return 0, 0, err
	}
	slotCount, err := recordInt(record, "slotCount")
	if err != nil {
		return 0, 0, err
	}
	return commitSeq, slotCount, nil
}

func (l *FragmentLoader) metaIntTx(tx *sql.Tx, key string) (int64, error) {
	var v string
	row := tx.QueryRow(`SELECT value FROM `+metaTable+` WHERE key = ?`, key)
	if err := row.Scan(&v); err != nil {
		return 0, fmt.Errorf("cannot read metadata %q: %w", key, err)
	}
	var n int64
	if _, err := fmt.Sscan(v, &n); err != nil {
		return 0, fmt.Errorf("cannot parse metadata %q value %q: %w", key, v, err)
	}
	return n, nil
}

func (l *FragmentLoader) setMetaTx(tx *sql.Tx, key, value string) error {
	if _, err := tx.Exec(
		`UPDATE `+metaTable+` SET value = ? WHERE key = ?`, value, key); err != nil {
		return fmt.Errorf("cannot update metadata %q: %w", key, err)
	}
	return nil
}

// recordString reads an Avro string field.
func recordString(record map[string]interface{}, field string) (string, error) {
	v, ok := record[field]
	if !ok {
		return "", fmt.Errorf("record missing %q field", field)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("field %q is %T, expected string", field, v)
	}
	return s, nil
}

// recordLong reads an Avro long field (decoded as int64 by goavro).
func recordLong(record map[string]interface{}, field string) (int64, error) {
	v, ok := record[field]
	if !ok {
		return 0, fmt.Errorf("record missing %q field", field)
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("field %q is %T, expected int64", field, v)
	}
	return n, nil
}

// recordInt reads an Avro int field (decoded as int32 by goavro).
func recordInt(record map[string]interface{}, field string) (int32, error) {
	v, ok := record[field]
	if !ok {
		return 0, fmt.Errorf("record missing %q field", field)
	}
	n, ok := v.(int32)
	if !ok {
		return 0, fmt.Errorf("field %q is %T, expected int32", field, v)
	}
	return n, nil
}

// sqlArgs builds the ordered parameter list for an INSERT following the
// binding's Columns order.
func sqlArgs(columns []string, row map[string]interface{}) ([]interface{}, error) {
	args := make([]interface{}, len(columns))
	for i, c := range columns {
		v, err := sqlValue(row[c])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c, err)
		}
		args[i] = v
	}
	return args, nil
}

// sqlValue converts a decoded Avro row value into a database/sql parameter.
// Union values are single-entry maps keyed by their Avro type name; NULL
// stays a plain nil so NULL/0/false remain distinct; TxDecimal records are
// rendered as exact decimal strings so precision and scale are preserved
// without floating-point conversion.
func sqlValue(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case persistedDecimal:
		return t.text, nil
	case *big.Rat:
		return decimalString(t), nil
	case map[string]interface{}:
		if isTxDecimalRecord(t) {
			d, err := txDecimalFromRecord(t)
			if err != nil {
				return nil, err
			}
			return d.text, nil
		}
		if len(t) != 1 {
			return nil, fmt.Errorf("union value must have exactly one type key, got %d", len(t))
		}
		for _, inner := range t {
			return sqlValue(inner)
		}
	case bool:
		return t, nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case string:
		return t, nil
	case []byte:
		return t, nil
	}
	return nil, fmt.Errorf("unsupported row value type %T", v)
}

// decimalString renders an exact Avro decimal (*big.Rat) as its shortest
// exact finite decimal representation without floating-point conversion.
func decimalString(r *big.Rat) string {
	if r == nil {
		return ""
	}
	denom := new(big.Int).Set(r.Denom())
	two := big.NewInt(2)
	five := big.NewInt(5)
	var twos, fives int
	rem := new(big.Int)
	for rem.Mod(denom, two); rem.Sign() == 0; rem.Mod(denom, two) {
		denom.Div(denom, two)
		twos++
	}
	for rem.Mod(denom, five); rem.Sign() == 0; rem.Mod(denom, five) {
		denom.Div(denom, five)
		fives++
	}
	scale := twos
	if fives > scale {
		scale = fives
	}
	return r.FloatString(scale)
}

// isTxDecimalRecord recognizes the decoded TxDecimal record shape
// (map with exactly the unscaled/scale fields).
func isTxDecimalRecord(m map[string]interface{}) bool {
	_, hasUnscaled := m["unscaled"]
	_, hasScale := m["scale"]
	return hasUnscaled && hasScale && len(m) == 2
}

// txDecimalFromRecord converts a decoded TxDecimal record (two's-complement
// unscaled bytes plus scale) into an exact decimal text value.
func txDecimalFromRecord(m map[string]interface{}) (persistedDecimal, error) {
	rawUnscaled, ok := m["unscaled"].([]byte)
	if !ok {
		return persistedDecimal{}, fmt.Errorf("TxDecimal.unscaled is %T, expected []byte", m["unscaled"])
	}
	scale, ok := m["scale"].(int32)
	if !ok {
		return persistedDecimal{}, fmt.Errorf("TxDecimal.scale is %T, expected int32", m["scale"])
	}
	unscaled := new(big.Int)
	fromSignedBytes(unscaled, rawUnscaled)
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat := new(big.Rat).SetFrac(unscaled, denom)
	return persistedDecimal{text: rat.FloatString(int(scale)), scale: int(scale), rat: rat}, nil
}

// persistedDecimal carries the exact decimal text (with the original scale)
// and the rational value used for canonical comparison after a reopen.
type persistedDecimal struct {
	text  string
	scale int
	rat   *big.Rat
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// canonicalValue normalizes a decoded Avro value into deterministic JSON so
// identical retransmissions compare equal regardless of map iteration order.
func canonicalValue(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case map[string]interface{}:
		if isTxDecimalRecord(t) {
			d, err := txDecimalFromRecord(t)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"__decimal__": d.text,
				"scale":       d.scale,
			}, nil
		}
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			cv, err := canonicalValue(val)
			if err != nil {
				return nil, err
			}
			out[k] = cv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			cv, err := canonicalValue(val)
			if err != nil {
				return nil, err
			}
			out[i] = cv
		}
		return out, nil
	case *big.Rat:
		return map[string]interface{}{"__decimal__": decimalString(t)}, nil
	case []byte:
		return map[string]interface{}{"__bytes__": base64.StdEncoding.EncodeToString(t)}, nil
	case bool, string, float32, float64, int32, int64, int:
		return t, nil
	default:
		return nil, fmt.Errorf("cannot canonicalize value of type %T", v)
	}
}

// canonicalPayload is the persisted fingerprint of a data fragment.  The
// slot/table/op are folded into the payload JSON so a single string fully
// identifies the fragment.
func canonicalPayload(slot int32, tableName, op string, row map[string]interface{}) (string, error) {
	crow, err := canonicalValue(row)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(struct {
		Slot  int32       `json:"slot"`
		Table string      `json:"table"`
		Op    string      `json:"op"`
		Row   interface{} `json:"row"`
	}{slot, tableName, op, crow})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func canonicalCommit(commitSeq int64, slotCount int32) (string, error) {
	b, err := json.Marshal(struct {
		CommitSeq int64 `json:"commitSeq"`
		SlotCount int32 `json:"slotCount"`
	}{commitSeq, slotCount})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodePayloadRow extracts the row map from a canonical data payload and
// revives typed markers (decimals, bytes) introduced by canonicalValue.
func decodePayloadRow(payload string) (map[string]interface{}, error) {
	var envelope struct {
		Slot int32           `json:"slot"`
		Row  json.RawMessage `json:"row"`
	}
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		return nil, fmt.Errorf("cannot decode persisted payload: %w", err)
	}
	// Use json.Number so int64 values beyond 2^53 survive a reopen without
	// being rounded through float64.
	dec := json.NewDecoder(strings.NewReader(string(envelope.Row)))
	dec.UseNumber()
	var raw map[string]interface{}
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("cannot decode persisted row: %w", err)
	}
	row := make(map[string]interface{}, len(raw))
	for k, v := range raw {
		revived, err := reviveValue(v)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", k, err)
		}
		row[k] = revived
	}
	return row, nil
}

// reviveValue reverses the markers introduced by canonicalValue.  Integral
// json.Number values come back as int64 (never rounded through float64);
// non-integral numbers stay float64.
func reviveValue(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, err := t.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid persisted number %q: %w", t.String(), err)
		}
		return f, nil
	case map[string]interface{}:
		if d, ok := t["__decimal__"]; ok {
			s, _ := d.(string)
			r, ok := new(big.Rat).SetString(s)
			if !ok {
				return nil, fmt.Errorf("invalid persisted decimal %q", s)
			}
			scale := 0
			// scale is a json.Number when decoded with UseNumber.
			switch sc := t["scale"].(type) {
			case json.Number:
				if n, err := sc.Int64(); err == nil {
					scale = int(n)
				}
			case float64:
				scale = int(sc)
			}
			return persistedDecimal{text: s, scale: scale, rat: r}, nil
		}
		if b, ok := t["__bytes__"]; ok {
			s, _ := b.(string)
			return base64.StdEncoding.DecodeString(s)
		}
		// Plain (union) map: revive its entries.
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			rv, err := reviveValue(val)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, val := range t {
			rv, err := reviveValue(val)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return v, nil
	}
}

// PendingFragments returns the counts of persisted-but-not-applied data and
// commit fragments.  It is primarily useful for tests and operational
// diagnostics after a reopen.
func (l *FragmentLoader) PendingFragments() (data int64, commits int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err = l.db.QueryRow(
		`SELECT COUNT(*) FROM ` + fragmentTable + ` WHERE applied = 0`).Scan(&data); err != nil {
		return 0, 0, err
	}
	head, err := l.readMetaInt(metaKeyApplied)
	if err != nil {
		return 0, 0, err
	}
	if err = l.db.QueryRow(
		`SELECT COUNT(*) FROM `+commitTable+` WHERE commit_seq > ?`, head).Scan(&commits); err != nil {
		return 0, 0, err
	}
	return data, commits, nil
}
