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
	"bytes"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

// TestFragmentLoader_LargeInt64PrecisionAcrossReopen ensures a long beyond
// 2^53 round-trips exactly through the persisted fragment store.
func TestFragmentLoader_LargeInt64PrecisionAcrossReopen(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	const bigID int64 = 9007199254740993 // 2^53 + 2, not exactly float64-representable
	row := parentRow(bigID, "big", nil)
	if _, err := l.Ingest(dataWire(t, r, 1100, 0, "parent", "I", row)); err != nil {
		t.Fatalf("data: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 1100, 1, 1)); err != nil {
		t.Fatalf("commit: %s", err)
	}
	l.Close()

	l2 := newLoader(t, dsn)
	// Replay the identical message: must be idempotent, not re-apply.
	if _, err := l2.Ingest(dataWire(t, r, 1100, 0, "parent", "I", row)); err != nil {
		t.Fatalf("replay data: %s", err)
	}
	if _, err := l2.Ingest(commitWire(t, r, 1100, 1, 1)); err != nil {
		t.Fatalf("replay commit: %s", err)
	}
	applied, err := l2.Pump()
	if err != nil {
		t.Fatalf("pump: %s", err)
	}
	if len(applied) != 0 {
		t.Fatalf("reopen replay re-applied %v", applied)
	}
	var got int64
	if err := openRawDB(t, dsn).QueryRow(`SELECT id FROM parent WHERE label = 'big'`).Scan(&got); err != nil {
		t.Fatalf("query big id: %s", err)
	}
	if got != bigID {
		t.Fatalf("id = %d, want exact %d (float64 rounding suspected)", got, bigID)
	}
}

// TestFragmentLoader_BytesWrittenToBlob verifies Avro bytes land in a BLOB
// column verbatim (distinct from a text column) and survive a reopen.
func TestFragmentLoader_BytesWrittenToBlob(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "blob.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	if _, err := db.Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)`); err != nil {
		t.Fatalf("create: %s", err)
	}
	db.Close()

	r := testRegistry(t)
	l, err := NewFragmentLoader(FragmentLoaderConfig{
		DSN:      dsn,
		Registry: r,
		Bindings: map[string]TableBinding{
			"docs": {Table: "docs", Columns: []string{"id", "payload"}, PrimaryKey: []string{"id"}},
		},
	})
	if err != nil {
		t.Fatalf("NewFragmentLoader: %s", err)
	}
	defer l.Close()

	raw := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	row := map[string]interface{}{
		"id":      map[string]interface{}{"long": int64(1)},
		"payload": map[string]interface{}{"bytes": raw},
	}
	if _, err := l.Ingest(dataWire(t, r, 1200, 0, "docs", "I", row)); err != nil {
		t.Fatalf("data: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 1200, 1, 1)); err != nil {
		t.Fatalf("commit: %s", err)
	}
	var stored []byte
	if err := openRawDB(t, dsn).QueryRow(`SELECT payload FROM docs WHERE id = 1`).Scan(&stored); err != nil {
		t.Fatalf("query blob: %s", err)
	}
	if !bytes.Equal(stored, raw) {
		t.Fatalf("blob = %v, want %v", stored, raw)
	}
}

// TestFragmentLoader_WriteFailureRollsBackAndBlocks covers a non-FK write
// failure (NOT NULL violation): the transaction rolls back, the successful
// prefix stays, later transactions remain blocked, and Pump retries after
// the schema problem is repaired.
func TestFragmentLoader_WriteFailureRollsBackAndBlocks(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "notnull.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	if _, err := db.Exec(`CREATE TABLE nn (
		id INTEGER PRIMARY KEY,
		required TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create: %s", err)
	}
	db.Close()

	r := testRegistry(t)
	l, err := NewFragmentLoader(FragmentLoaderConfig{
		DSN:      dsn,
		Registry: r,
		Bindings: map[string]TableBinding{
			"nn": {Table: "nn", Columns: []string{"id", "required"}, PrimaryKey: []string{"id"}},
		},
	})
	if err != nil {
		t.Fatalf("NewFragmentLoader: %s", err)
	}
	defer l.Close()

	// commitSeq 1 succeeds.
	okRow := map[string]interface{}{
		"id":       map[string]interface{}{"long": int64(1)},
		"required": uStr("yes"),
	}
	if _, err := l.Ingest(dataWire(t, r, 1300, 0, "nn", "I", okRow)); err != nil {
		t.Fatalf("good data: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 1300, 1, 1)); err != nil {
		t.Fatalf("good commit: %s", err)
	}

	// commitSeq 2 violates NOT NULL.
	badRow := map[string]interface{}{
		"id":       map[string]interface{}{"long": int64(2)},
		"required": nil,
	}
	if _, err := l.Ingest(dataWire(t, r, 1301, 0, "nn", "I", badRow)); err != nil {
		t.Fatalf("bad data ingest must persist: %s", err)
	}
	res, err := l.Ingest(commitWire(t, r, 1301, 2, 1))
	if err != nil {
		t.Fatalf("bad commit ingest must persist: %s", err)
	}
	var blocked *ErrBlocked
	if !errors.As(res.PumpError, &blocked) {
		t.Fatalf("PumpError = %v, want ErrBlocked", res.PumpError)
	}

	// Prefix retained, failed tx rolled back.
	raw := openRawDB(t, dsn)
	var n int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM nn`).Scan(&n); err != nil {
		t.Fatalf("count: %s", err)
	}
	if n != 1 {
		t.Fatalf("row count = %d, want 1 (prefix only)", n)
	}
	head, err := l.AppliedHead()
	if err != nil {
		t.Fatalf("head: %s", err)
	}
	if head != 1 {
		t.Fatalf("head = %d, want 1", head)
	}

	// A later complete tx stays blocked behind the failed one.
	laterRow := map[string]interface{}{
		"id":       map[string]interface{}{"long": int64(3)},
		"required": uStr("later"),
	}
	if _, err := l.Ingest(dataWire(t, r, 1302, 0, "nn", "I", laterRow)); err != nil {
		t.Fatalf("later data: %s", err)
	}
	res2, err := l.Ingest(commitWire(t, r, 1302, 3, 1))
	if err != nil {
		t.Fatalf("later commit: %s", err)
	}
	if !errors.As(res2.PumpError, &blocked) {
		t.Fatalf("later PumpError = %v, want ErrBlocked", res2.PumpError)
	}

	// Repair the schema so the previously bad row is now legal (rebuild the
	// table with a nullable required column), then Pump applies seq 2 and
	// seq 3 in order.
	if _, err := raw.Exec(`CREATE TABLE nn_new (id INTEGER PRIMARY KEY, required TEXT)`); err != nil {
		t.Fatalf("create rebuilt table: %s", err)
	}
	if _, err := raw.Exec(`INSERT INTO nn_new (id, required) SELECT id, required FROM nn`); err != nil {
		t.Fatalf("copy rows: %s", err)
	}
	if _, err := raw.Exec(`DROP TABLE nn`); err != nil {
		t.Fatalf("drop old table: %s", err)
	}
	if _, err := raw.Exec(`ALTER TABLE nn_new RENAME TO nn`); err != nil {
		t.Fatalf("rename rebuilt table: %s", err)
	}
	applied, err := l.Pump()
	if err != nil {
		t.Fatalf("Pump after repair: %s", err)
	}
	if len(applied) != 2 || applied[0] != 2 || applied[1] != 3 {
		t.Fatalf("applied = %v, want [2 3]", applied)
	}
	if err := raw.QueryRow(`SELECT COUNT(*) FROM nn`).Scan(&n); err != nil {
		t.Fatalf("final count: %s", err)
	}
	if n != 3 {
		t.Fatalf("final row count = %d, want 3", n)
	}
}
