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
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sync"
	"testing"
)

// ---- test helpers ---------------------------------------------------------

func testRegistry(t *testing.T) *SchemaRegistry {
	t.Helper()
	r, err := NewDefaultSchemaRegistry()
	if err != nil {
		t.Fatalf("NewDefaultSchemaRegistry: %s", err)
	}
	return r
}

func testBindings() map[string]TableBinding {
	return map[string]TableBinding{
		"parent": {
			Table:      "parent",
			Columns:    []string{"id", "label", "amount"},
			PrimaryKey: []string{"id"},
		},
		"child": {
			Table:      "child",
			Columns:    []string{"id", "parent_id", "note"},
			PrimaryKey: []string{"id"},
		},
	}
}

// newTestDB creates a fresh on-disk SQLite database with target tables whose
// foreign key is NOT declared DEFERRABLE, so defer_foreign_keys must do the
// deferral work at runtime.
func newTestDB(t *testing.T) string {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "loader.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open test db: %s", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE parent (
			id INTEGER PRIMARY KEY,
			label TEXT,
			amount TEXT
		)`,
		`CREATE TABLE child (
			id INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL REFERENCES parent(id),
			note TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create target table: %s", err)
		}
	}
	return dsn
}

func newLoader(t *testing.T, dsn string) *FragmentLoader {
	t.Helper()
	l, err := NewFragmentLoader(FragmentLoaderConfig{
		DSN:      dsn,
		Registry: testRegistry(t),
		Bindings: testBindings(),
	})
	if err != nil {
		t.Fatalf("NewFragmentLoader: %s", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func openRawDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("reopen db: %s", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// txDecimalRecord builds a native TxDecimal record from an unscaled value and
// a scale, e.g. (4020, 2) represents 40.20.
func txDecimalRecord(t *testing.T, unscaled int64, scale int) map[string]interface{} {
	t.Helper()
	n := big.NewInt(unscaled)
	raw, err := toSignedBytes(n)
	if err != nil {
		t.Fatalf("toSignedBytes: %s", err)
	}
	return map[string]interface{}{
		"unscaled": raw,
		"scale":    int32(scale),
	}
}

func dataWire(t *testing.T, r *SchemaRegistry, txID int64, slot int32, table, op string, row map[string]interface{}) []byte {
	t.Helper()
	codec, err := r.Codec(TxDataSchemaID)
	if err != nil {
		t.Fatalf("codec lookup: %s", err)
	}
	datum := map[string]interface{}{
		"kind":  "DATA",
		"txID":  txID,
		"slot":  slot,
		"table": table,
		"op":    op,
		"row":   row,
	}
	wire, err := codec.EncodeWire(nil, TxDataSchemaID, datum)
	if err != nil {
		t.Fatalf("EncodeWire data: %s", err)
	}
	return wire
}

func commitWire(t *testing.T, r *SchemaRegistry, txID, commitSeq int64, slotCount int32) []byte {
	t.Helper()
	codec, err := r.Codec(TxCommitSchemaID)
	if err != nil {
		t.Fatalf("codec lookup: %s", err)
	}
	datum := map[string]interface{}{
		"kind":      "COMMIT",
		"txID":      txID,
		"commitSeq": commitSeq,
		"slotCount": slotCount,
	}
	wire, err := codec.EncodeWire(nil, TxCommitSchemaID, datum)
	if err != nil {
		t.Fatalf("EncodeWire commit: %s", err)
	}
	return wire
}

// wrapUnion coerces bare Go scalars into the Avro union map shape; nil and
// already-wrapped union maps pass through unchanged.
func wrapUnion(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return uStr(t)
	case bool:
		return uBool(t)
	default:
		return v
	}
}

func parentRow(id int64, label interface{}, amount interface{}) map[string]interface{} {
	return map[string]interface{}{
		"id":     map[string]interface{}{"long": id},
		"label":  wrapUnion(label),
		"amount": wrapUnion(amount),
	}
}

func childRow(id, parentID int64, note interface{}) map[string]interface{} {
	return map[string]interface{}{
		"id":        map[string]interface{}{"long": id},
		"parent_id": map[string]interface{}{"long": parentID},
		"note":      wrapUnion(note),
	}
}

// uStr/uBool wrap plain values in the union map shape Avro encoding expects.
func uStr(s string) interface{} { return map[string]interface{}{"string": s} }
func uBool(b bool) interface{}  { return map[string]interface{}{"boolean": b} }

func queryParent(t *testing.T, db *sql.DB, id int64) (label, amount sql.NullString) {
	t.Helper()
	err := db.QueryRow(`SELECT label, amount FROM parent WHERE id = ?`, id).Scan(&label, &amount)
	if err != nil {
		t.Fatalf("query parent %d: %s", id, err)
	}
	return
}

func assertChildCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM child`).Scan(&n); err != nil {
		t.Fatalf("count child: %s", err)
	}
	if n != want {
		t.Fatalf("child count = %d, want %d", n, want)
	}
}

func assertParentCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM parent`).Scan(&n); err != nil {
		t.Fatalf("count parent: %s", err)
	}
	if n != want {
		t.Fatalf("parent count = %d, want %d", n, want)
	}
}

// ---- tests ----------------------------------------------------------------

func TestFragmentLoader_InterleavedApplyInCommitOrder(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// tx 100 and tx 200 fragments interleaved; tx 200 fully arrives first.
	res, err := l.Ingest(dataWire(t, r, 200, 0, "parent", "I",
		parentRow(2, "two", nil)))
	if err != nil {
		t.Fatalf("ingest tx200 slot0: %s", err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("nothing should be applied yet, got %v", res.Applied)
	}
	if _, err := l.Ingest(commitWire(t, r, 200, 2, 1)); err != nil {
		t.Fatalf("ingest tx200 commit: %s", err)
	}
	// tx 200 is complete and persisted, but commitSeq 2 must wait for seq 1.
	head, err := l.AppliedHead()
	if err != nil {
		t.Fatalf("AppliedHead: %s", err)
	}
	if head != 0 {
		t.Fatalf("applied head = %d, want 0 (commit order must be respected)", head)
	}
	assertParentCount(t, openRawDB(t, dsn), 0)

	if _, err := l.Ingest(dataWire(t, r, 100, 0, "parent", "I",
		parentRow(1, "one", nil))); err != nil {
		t.Fatalf("ingest tx100 slot0: %s", err)
	}
	res, err = l.Ingest(commitWire(t, r, 100, 1, 1))
	if err != nil {
		t.Fatalf("ingest tx100 commit: %s", err)
	}
	// One pump should apply BOTH transactions in order 1 then 2.
	if len(res.Applied) != 2 || res.Applied[0] != 1 || res.Applied[1] != 2 {
		t.Fatalf("applied seqs = %v, want [1 2]", res.Applied)
	}
	if res.AppliedHead != 2 {
		t.Fatalf("applied head = %d, want 2", res.AppliedHead)
	}
	db := openRawDB(t, dsn)
	assertParentCount(t, db, 2)
}

func TestFragmentLoader_CommitFirstThenData(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	res, err := l.Ingest(commitWire(t, r, 300, 1, 1))
	if err != nil {
		t.Fatalf("commit first: %s", err)
	}
	if res.AppliedHead != 0 {
		t.Fatalf("head = %d, want 0 while slots missing", res.AppliedHead)
	}
	res, err = l.Ingest(dataWire(t, r, 300, 0, "parent", "I",
		parentRow(7, "seven", nil)))
	if err != nil {
		t.Fatalf("data after commit: %s", err)
	}
	if res.AppliedHead != 1 || len(res.Applied) != 1 {
		t.Fatalf("head = %d applied = %v, want head 1 applied [1]", res.AppliedHead, res.Applied)
	}
}

func TestFragmentLoader_EmptyTransactionAdvancesHead(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	res, err := l.Ingest(commitWire(t, r, 400, 1, 0))
	if err != nil {
		t.Fatalf("empty tx commit: %s", err)
	}
	if res.AppliedHead != 1 || len(res.Applied) != 1 {
		t.Fatalf("empty tx: head = %d applied = %v", res.AppliedHead, res.Applied)
	}
	// A following normal transaction still works.
	if _, err := l.Ingest(dataWire(t, r, 401, 0, "parent", "I",
		parentRow(1, "x", nil))); err != nil {
		t.Fatalf("data: %s", err)
	}
	res, err = l.Ingest(commitWire(t, r, 401, 2, 1))
	if err != nil {
		t.Fatalf("commit: %s", err)
	}
	if res.AppliedHead != 2 {
		t.Fatalf("head = %d, want 2", res.AppliedHead)
	}
}

func TestFragmentLoader_DuplicateIdenticalIsIdempotent(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	wire := dataWire(t, r, 500, 0, "parent", "I", parentRow(1, "a", nil))
	res, err := l.Ingest(wire)
	if err != nil {
		t.Fatalf("first data ingest: %s", err)
	}
	if res.Duplicate {
		t.Fatal("first ingest must not be a duplicate")
	}
	res, err = l.Ingest(wire)
	if err != nil {
		t.Fatalf("identical data re-delivery: %s", err)
	}
	if !res.Duplicate {
		t.Fatal("identical re-delivery must be reported as duplicate")
	}

	cwire := commitWire(t, r, 500, 1, 1)
	if _, err := l.Ingest(cwire); err != nil {
		t.Fatalf("first commit: %s", err)
	}
	res, err = l.Ingest(cwire)
	if err != nil {
		t.Fatalf("identical commit re-delivery: %s", err)
	}
	if !res.Duplicate {
		t.Fatal("identical commit re-delivery must be a duplicate")
	}

	// Reopening and replaying the same messages must not double-apply.
	l.Close()
	l2 := newLoader(t, dsn)
	head, err := l2.AppliedHead()
	if err != nil {
		t.Fatalf("reopen AppliedHead: %s", err)
	}
	if head != 1 {
		t.Fatalf("head after reopen = %d, want 1", head)
	}
	if _, err := l2.Ingest(wire); err != nil {
		t.Fatalf("replay data after reopen: %s", err)
	}
	if _, err := l2.Ingest(cwire); err != nil {
		t.Fatalf("replay commit after reopen: %s", err)
	}
	applied, err := l2.Pump()
	if err != nil {
		t.Fatalf("pump after reopen: %s", err)
	}
	if len(applied) != 0 {
		t.Fatalf("pump re-applied %v; nothing should be re-applied", applied)
	}
	db := openRawDB(t, dsn)
	assertParentCount(t, db, 1)
}

func TestFragmentLoader_ConflictDifferentContentRejected(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	w1 := dataWire(t, r, 600, 0, "parent", "I", parentRow(1, "first", nil))
	if _, err := l.Ingest(w1); err != nil {
		t.Fatalf("first data: %s", err)
	}
	w2 := dataWire(t, r, 600, 0, "parent", "I", parentRow(1, "second", nil))
	_, err := l.Ingest(w2)
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("different content same slot: want ErrConflict, got %v", err)
	}

	// Same slot but different table/op is also a conflict (different payload).
	w3 := dataWire(t, r, 600, 0, "parent", "U", parentRow(1, "first", nil))
	if _, err := l.Ingest(w3); err == nil {
		t.Fatal("different op for same slot must conflict")
	}

	// Commit conflict: same txID, different slotCount.
	if _, err := l.Ingest(commitWire(t, r, 600, 1, 1)); err != nil {
		t.Fatalf("first commit: %s", err)
	}
	_, err = l.Ingest(commitWire(t, r, 600, 1, 2))
	if !errors.As(err, &conflict) {
		t.Fatalf("different slotCount: want ErrConflict, got %v", err)
	}

	// Reused commitSeq on a different txID.
	_, err = l.Ingest(commitWire(t, r, 601, 1, 0))
	if !errors.As(err, &conflict) {
		t.Fatalf("reused commitSeq: want ErrConflict, got %v", err)
	}

	// Receive cursor must not have advanced on the rejected messages beyond
	// the four accepted ones (data + commit + ...).
	pos, err := l.ReceivePosition()
	if err != nil {
		t.Fatalf("ReceivePosition: %s", err)
	}
	if pos != 2 {
		t.Fatalf("receive cursor = %d, want 2 (conflicts must not advance it)", pos)
	}
}

func TestFragmentLoader_ReceiveAndAppliedProgressAreSeparate(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Accept data for tx 700 without its commit: receive moves, applied does not.
	for slot := int32(0); slot < 3; slot++ {
		if _, err := l.Ingest(dataWire(t, r, 700, slot, "parent", "I",
			parentRow(int64(slot), fmt.Sprintf("n%d", slot), nil))); err != nil {
			t.Fatalf("data slot %d: %s", slot, err)
		}
	}
	pos, err := l.ReceivePosition()
	if err != nil {
		t.Fatalf("ReceivePosition: %s", err)
	}
	if pos != 3 {
		t.Fatalf("receive position = %d, want 3", pos)
	}
	head, err := l.AppliedHead()
	if err != nil {
		t.Fatalf("AppliedHead: %s", err)
	}
	if head != 0 {
		t.Fatalf("applied head = %d, must stay 0 without commit", head)
	}
	assertParentCount(t, openRawDB(t, dsn), 0)

	// Now commit; applied catches up in one transaction.
	res, err := l.Ingest(commitWire(t, r, 700, 1, 3))
	if err != nil {
		t.Fatalf("commit: %s", err)
	}
	if res.ReceivePosition != 4 || res.AppliedHead != 1 {
		t.Fatalf("receive = %d applied = %d, want 4 and 1", res.ReceivePosition, res.AppliedHead)
	}
}

func TestFragmentLoader_PersistBeforeApplyAndReopenRecovery(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Two transactions; close while the second is only partially received.
	if _, err := l.Ingest(dataWire(t, r, 800, 0, "parent", "I",
		parentRow(1, "first", nil))); err != nil {
		t.Fatalf("tx800 data: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 800, 1, 1)); err != nil {
		t.Fatalf("tx800 commit: %s", err)
	}
	if _, err := l.Ingest(dataWire(t, r, 801, 1, "parent", "I",
		parentRow(3, "third", nil))); err != nil {
		t.Fatalf("tx801 slot1: %s", err)
	}
	l.Close()

	// Reopen: applied head and pending fragments must recover.
	l2 := newLoader(t, dsn)
	head, err := l2.AppliedHead()
	if err != nil {
		t.Fatalf("reopen head: %s", err)
	}
	if head != 1 {
		t.Fatalf("head after reopen = %d, want 1", head)
	}
	pendingData, pendingCommits, err := l2.PendingFragments()
	if err != nil {
		t.Fatalf("PendingFragments: %s", err)
	}
	if pendingData != 1 || pendingCommits != 0 {
		t.Fatalf("pending = data:%d commits:%d, want data:1 commits:0", pendingData, pendingCommits)
	}

	// Deliver the missing slot then the commit; no duplicate application.
	if _, err := l2.Ingest(dataWire(t, r, 801, 0, "parent", "I",
		parentRow(2, "second", nil))); err != nil {
		t.Fatalf("tx801 slot0: %s", err)
	}
	res, err := l2.Ingest(commitWire(t, r, 801, 2, 2))
	if err != nil {
		t.Fatalf("tx801 commit: %s", err)
	}
	if len(res.Applied) != 1 || res.Applied[0] != 2 {
		t.Fatalf("applied = %v, want [2]", res.Applied)
	}
	assertParentCount(t, openRawDB(t, dsn), 3)
}

func TestFragmentLoader_SlotOrderAppliedWithinTransaction(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Two slots for the same row: slot 0 inserts, slot 1 updates.  Deliver
	// out of order; application must follow slot order.
	if _, err := l.Ingest(dataWire(t, r, 900, 1, "parent", "U",
		parentRow(1, "updated", nil))); err != nil {
		t.Fatalf("slot1: %s", err)
	}
	if _, err := l.Ingest(dataWire(t, r, 900, 0, "parent", "I",
		parentRow(1, "original", nil))); err != nil {
		t.Fatalf("slot0: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 900, 1, 2)); err != nil {
		t.Fatalf("commit: %s", err)
	}
	db := openRawDB(t, dsn)
	label, _ := queryParent(t, db, 1)
	if label.String != "updated" {
		t.Fatalf("label = %q, want %q (slot order not respected)", label.String, "updated")
	}
}

func TestFragmentLoader_DeleteUsesPrimaryKey(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Insert then delete in two transactions.
	if _, err := l.Ingest(dataWire(t, r, 910, 0, "parent", "I",
		parentRow(5, "keep", nil))); err != nil {
		t.Fatalf("insert: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 910, 1, 1)); err != nil {
		t.Fatalf("commit insert: %s", err)
	}
	// A DELETE row only carries primary-key columns.
	deleteRow := map[string]interface{}{"id": map[string]interface{}{"long": 5}}
	if _, err := l.Ingest(dataWire(t, r, 911, 0, "parent", "D", deleteRow)); err != nil {
		t.Fatalf("delete: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 911, 2, 1)); err != nil {
		t.Fatalf("commit delete: %s", err)
	}
	assertParentCount(t, openRawDB(t, dsn), 0)

	// DELETE rows missing the PK are rejected at ingest.
	_, err := l.Ingest(dataWire(t, r, 912, 0, "parent", "D",
		map[string]interface{}{"label": map[string]interface{}{"string": "x"}}))
	if err == nil {
		t.Fatal("DELETE without primary key must be rejected")
	}
}

func TestFragmentLoader_NullZeroFalseAndDecimalPreserved(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	row := map[string]interface{}{
		"id":     map[string]interface{}{"long": 0}, // zero PK
		"label":  nil,                               // explicit NULL, distinct from ""
		"amount": map[string]interface{}{"com.linkedin.goavro.tx.TxDecimal": txDecimalRecord(t, 4020, 2)},
	}
	if _, err := l.Ingest(dataWire(t, r, 920, 0, "parent", "I", row)); err != nil {
		t.Fatalf("insert edge values: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 920, 1, 1)); err != nil {
		t.Fatalf("commit: %s", err)
	}
	db := openRawDB(t, dsn)
	label, amount := queryParent(t, db, 0)
	if label.Valid {
		t.Fatalf("NULL label came back as valid %q", label.String)
	}
	if !amount.Valid || amount.String != "40.20" {
		t.Fatalf("amount = %#v, want exact 40.20", amount)
	}

	// false / 0 round trip through the child table.
	crow := childRow(0, 0, map[string]interface{}{"boolean": false})
	if _, err := l.Ingest(dataWire(t, r, 921, 0, "child", "I", crow)); err != nil {
		t.Fatalf("child insert: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 921, 2, 1)); err != nil {
		t.Fatalf("child commit: %s", err)
	}
	var note string
	if err := openRawDB(t, dsn).QueryRow(`SELECT note FROM child WHERE id = 0`).Scan(&note); err != nil {
		t.Fatalf("query child note: %s", err)
	}
	if note != "0" {
		t.Fatalf("boolean false stored as %q, want %q", note, "0")
	}

	// Negative decimal precision.
	negRow := parentRow(9, map[string]interface{}{"string": "neg"},
		map[string]interface{}{"com.linkedin.goavro.tx.TxDecimal": txDecimalRecord(t, -12345, 3)})
	if _, err := l.Ingest(dataWire(t, r, 922, 0, "parent", "I", negRow)); err != nil {
		t.Fatalf("negative decimal insert: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 922, 3, 1)); err != nil {
		t.Fatalf("negative decimal commit: %s", err)
	}
	_, amount = queryParent(t, openRawDB(t, dsn), 9)
	if amount.String != "-12.345" {
		t.Fatalf("negative decimal = %q, want -12.345", amount.String)
	}
}

func TestFragmentLoader_DeferredForeignKeyChildBeforeParent(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Slot 0: child referencing parent 1; slot 1: the parent itself.
	if _, err := l.Ingest(dataWire(t, r, 930, 0, "child", "I",
		childRow(1, 1, map[string]interface{}{"string": "c1"}))); err != nil {
		t.Fatalf("child first: %s", err)
	}
	if _, err := l.Ingest(dataWire(t, r, 930, 1, "parent", "I",
		parentRow(1, "p1", nil))); err != nil {
		t.Fatalf("parent second: %s", err)
	}
	res, err := l.Ingest(commitWire(t, r, 930, 1, 2))
	if err != nil {
		t.Fatalf("commit deferred-FK tx: %s", err)
	}
	if len(res.Applied) != 1 {
		t.Fatalf("applied = %v, want [1]", res.Applied)
	}
	db := openRawDB(t, dsn)
	assertChildCount(t, db, 1)
	assertParentCount(t, db, 1)
}

func TestFragmentLoader_ForeignKeyFailureRollsBackAndBlocks(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// tx 940: child points at a parent that never arrives in this tx ->
	// deferred FK violation at COMMIT.
	if _, err := l.Ingest(dataWire(t, r, 940, 0, "child", "I",
		childRow(1, 999, map[string]interface{}{"string": "orphan"}))); err != nil {
		t.Fatalf("orphan child: %s", err)
	}
	res, err := l.Ingest(commitWire(t, r, 940, 1, 1))
	if err != nil {
		t.Fatalf("ingest must succeed even when application is blocked: %s", err)
	}
	var blocked *ErrBlocked
	if !errors.As(res.PumpError, &blocked) {
		t.Fatalf("want ErrBlocked on FK violation, got PumpError %v", res.PumpError)
	}
	if blocked.CommitSeq != 1 {
		t.Fatalf("blocked commitSeq = %d, want 1", blocked.CommitSeq)
	}
	db := openRawDB(t, dsn)
	assertChildCount(t, db, 0) // rolled back
	head, err := l.AppliedHead()
	if err != nil {
		t.Fatalf("AppliedHead: %s", err)
	}
	if head != 0 {
		t.Fatalf("head = %d, must remain 0 after rollback", head)
	}

	// A subsequent complete transaction must stay blocked behind the head.
	if _, err := l.Ingest(dataWire(t, r, 941, 0, "parent", "I",
		parentRow(1, "later", nil))); err != nil {
		t.Fatalf("later tx data ingest must still succeed (persisted): %s", err)
	}
	res, err = l.Ingest(commitWire(t, r, 941, 2, 1))
	if err != nil {
		t.Fatalf("later tx commit ingest must still succeed (persisted): %s", err)
	}
	if !errors.As(res.PumpError, &blocked) {
		t.Fatalf("PumpError = %v, want ErrBlocked", res.PumpError)
	}
	assertParentCount(t, db, 0) // head still blocked

	// Pending fragments survive and the failed tx is still there for retry.
	pendingData, pendingCommits, err := l.PendingFragments()
	if err != nil {
		t.Fatalf("PendingFragments: %s", err)
	}
	if pendingData != 2 || pendingCommits != 2 {
		t.Fatalf("pending = data:%d commits:%d, want 2 and 2", pendingData, pendingCommits)
	}

	// Fix the blocking condition by inserting the missing parent out of
	// band, then Pump: tx 940 now commits and tx 941 follows automatically.
	if _, err := db.Exec(`INSERT INTO parent (id, label) VALUES (999, 'repair')`); err != nil {
		t.Fatalf("repair parent: %s", err)
	}
	applied, err := l.Pump()
	if err != nil {
		t.Fatalf("Pump after repair: %s", err)
	}
	if len(applied) != 2 || applied[0] != 1 || applied[1] != 2 {
		t.Fatalf("applied after repair = %v, want [1 2]", applied)
	}
	assertChildCount(t, openRawDB(t, dsn), 1)
}

func TestFragmentLoader_ReopenAfterBlockKeepsPrefix(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// tx 1 applied successfully.
	if _, err := l.Ingest(dataWire(t, r, 950, 0, "parent", "I",
		parentRow(1, "ok", nil))); err != nil {
		t.Fatalf("tx950 data: %s", err)
	}
	if _, err := l.Ingest(commitWire(t, r, 950, 1, 1)); err != nil {
		t.Fatalf("tx950 commit: %s", err)
	}
	// tx 2 fails on FK.
	if _, err := l.Ingest(dataWire(t, r, 951, 0, "child", "I",
		childRow(1, 999, nil))); err != nil {
		t.Fatalf("tx951 data: %s", err)
	}
	res, err := l.Ingest(commitWire(t, r, 951, 2, 1))
	if err != nil {
		t.Fatalf("ingest must succeed even when application is blocked: %s", err)
	}
	var blocked *ErrBlocked
	if !errors.As(res.PumpError, &blocked) {
		t.Fatalf("want ErrBlocked, got PumpError %v", res.PumpError)
	}
	l.Close()

	// Reopen: the successful prefix is retained, the blocked tx is pending.
	l2 := newLoader(t, dsn)
	head, err := l2.AppliedHead()
	if err != nil {
		t.Fatalf("reopen head: %s", err)
	}
	if head != 1 {
		t.Fatalf("head after reopen = %d, want 1 (prefix retained)", head)
	}
	assertParentCount(t, openRawDB(t, dsn), 1)
	assertChildCount(t, openRawDB(t, dsn), 0)
	pendingData, pendingCommits, err := l2.PendingFragments()
	if err != nil {
		t.Fatalf("PendingFragments: %s", err)
	}
	if pendingData != 1 || pendingCommits != 1 {
		t.Fatalf("pending = data:%d commits:%d, want 1 and 1", pendingData, pendingCommits)
	}
}

func TestFragmentLoader_IngestAtExplicitPositions(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	w := dataWire(t, r, 960, 0, "parent", "I", parentRow(1, "a", nil))
	res, err := l.IngestAt(100, w)
	if err != nil {
		t.Fatalf("IngestAt 100: %s", err)
	}
	if res.ReceivePosition != 100 {
		t.Fatalf("receive = %d, want 100", res.ReceivePosition)
	}
	// Auto-advance continues from the explicit position.
	res, err = l.Ingest(w)
	if err != nil {
		t.Fatalf("auto advance: %s", err)
	}
	if res.ReceivePosition != 101 {
		t.Fatalf("receive = %d, want 101", res.ReceivePosition)
	}
	// Stale positions are rejected.
	if _, err := l.IngestAt(50, w); err == nil {
		t.Fatal("stale receive position must be rejected")
	}
}

func TestFragmentLoader_OutOfRangeAndUnknownTableRejected(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	// Commit first announcing 1 slot, then a slot index out of range.
	if _, err := l.Ingest(commitWire(t, r, 970, 1, 1)); err != nil {
		t.Fatalf("commit: %s", err)
	}
	_, err := l.Ingest(dataWire(t, r, 970, 1, "parent", "I", parentRow(1, "x", nil)))
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("out-of-range slot: want ErrConflict, got %v", err)
	}

	// Unknown table.
	_, err = l.Ingest(dataWire(t, r, 971, 0, "nope", "I",
		map[string]interface{}{"id": map[string]interface{}{"long": 1}}))
	if err == nil {
		t.Fatal("unknown table must be rejected")
	}

	// Row with an unbound column.
	_, err = l.Ingest(dataWire(t, r, 972, 0, "parent", "I",
		map[string]interface{}{
			"id":     map[string]interface{}{"long": 1},
			"label":  nil,
			"amount": nil,
			"ghost":  map[string]interface{}{"string": "x"},
		}))
	if err == nil {
		t.Fatal("unbound column must be rejected")
	}
}

func TestFragmentLoader_ConcurrentIngestSerializes(t *testing.T) {
	dsn := newTestDB(t)
	r := testRegistry(t)
	l := newLoader(t, dsn)

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txID := int64(1000 + i)
			_, e1 := l.Ingest(dataWire(t, r, txID, 0, "parent", "I",
				parentRow(int64(i), fmt.Sprintf("p%d", i), nil)))
			_, e2 := l.Ingest(commitWire(t, r, txID, int64(i+1), 1))
			if e1 != nil {
				errs <- e1
				return
			}
			errs <- e2
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ingest: %s", err)
		}
	}
	// commitSeq values were arbitrary per goroutine; pump only applies the
	// contiguous prefix starting at 1.  Verify no corruption instead of
	// exact counts via bookkeeping.
	pos, err := l.ReceivePosition()
	if err != nil {
		t.Fatalf("ReceivePosition: %s", err)
	}
	if pos != 2*n {
		t.Fatalf("receive position = %d, want %d", pos, 2*n)
	}
}
