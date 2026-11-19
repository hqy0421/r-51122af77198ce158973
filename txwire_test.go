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
	"encoding/binary"
	"testing"
)

func TestWireRoundTripData(t *testing.T) {
	registry, err := NewDefaultSchemaRegistry()
	if err != nil {
		t.Fatalf("NewDefaultSchemaRegistry: %s", err)
	}
	dataCodec, err := registry.Codec(TxDataSchemaID)
	if err != nil {
		t.Fatalf("lookup data codec: %s", err)
	}

	datum := map[string]interface{}{
		"kind":  "DATA",
		"txID":  int64(77),
		"slot":  int32(2),
		"table": "orders",
		"op":    "I",
		"row": map[string]interface{}{
			"id":    map[string]interface{}{"long": int64(42)},
			"note":  nil, // explicit null union
			"flag":  map[string]interface{}{"boolean": false},
			"zero":  map[string]interface{}{"int": int32(0)},
			"price": map[string]interface{}{"com.linkedin.goavro.tx.TxDecimal": txDecimalRecord(t, 4020, 2)},
			"blob":  map[string]interface{}{"bytes": []byte{0xDE, 0xAD}},
			"name":  map[string]interface{}{"string": "widget"},
			"ratio": map[string]interface{}{"double": 1.5},
			"f":     map[string]interface{}{"float": float32(2.5)},
		},
	}
	wire, err := dataCodec.EncodeWire(nil, TxDataSchemaID, datum)
	if err != nil {
		t.Fatalf("EncodeWire: %s", err)
	}
	if wire[0] != 0x00 {
		t.Fatalf("magic byte = 0x%02x, want 0x00", wire[0])
	}
	if got := int32(binary.BigEndian.Uint32(wire[1:5])); got != TxDataSchemaID {
		t.Fatalf("schema ID = %d, want %d", got, TxDataSchemaID)
	}

	decoded, id, err := registry.DecodeWire(wire)
	if err != nil {
		t.Fatalf("DecodeWire: %s", err)
	}
	if id != TxDataSchemaID {
		t.Fatalf("decoded schema ID = %d, want %d", id, TxDataSchemaID)
	}
	rec, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded datum is %T", decoded)
	}
	if rec["txID"].(int64) != 77 || rec["slot"].(int32) != 2 || rec["table"] != "orders" || rec["op"] != "I" {
		t.Fatalf("scalar fields mismatch: %#v", rec)
	}
	row := rec["row"].(map[string]interface{})
	if row["note"] != nil {
		t.Fatalf("null value decoded as %#v, want nil", row["note"])
	}
	if row["flag"].(map[string]interface{})["boolean"] != false {
		t.Fatalf("false value not preserved: %#v", row["flag"])
	}
	if row["zero"].(map[string]interface{})["int"] != int32(0) {
		t.Fatalf("zero value not preserved: %#v", row["zero"])
	}
	blob := row["blob"].(map[string]interface{})["bytes"].([]byte)
	if !bytes.Equal(blob, []byte{0xDE, 0xAD}) {
		t.Fatalf("bytes mismatch: %v", blob)
	}
}

func TestWireRoundTripCommit(t *testing.T) {
	registry, err := NewDefaultSchemaRegistry()
	if err != nil {
		t.Fatalf("NewDefaultSchemaRegistry: %s", err)
	}
	commitCodec, err := registry.Codec(TxCommitSchemaID)
	if err != nil {
		t.Fatalf("lookup commit codec: %s", err)
	}
	datum := map[string]interface{}{
		"kind":      "COMMIT",
		"txID":      int64(9),
		"commitSeq": int64(3),
		"slotCount": int32(0), // empty transaction
	}
	wire, err := commitCodec.EncodeWire(nil, TxCommitSchemaID, datum)
	if err != nil {
		t.Fatalf("EncodeWire: %s", err)
	}
	decoded, id, err := registry.DecodeWire(wire)
	if err != nil {
		t.Fatalf("DecodeWire: %s", err)
	}
	if id != TxCommitSchemaID {
		t.Fatalf("schema ID = %d, want %d", id, TxCommitSchemaID)
	}
	rec := decoded.(map[string]interface{})
	if rec["commitSeq"].(int64) != 3 || rec["slotCount"].(int32) != 0 {
		t.Fatalf("commit fields mismatch: %#v", rec)
	}
}

func TestDecodeWireErrors(t *testing.T) {
	registry, err := NewDefaultSchemaRegistry()
	if err != nil {
		t.Fatalf("NewDefaultSchemaRegistry: %s", err)
	}
	if _, _, err := registry.DecodeWire([]byte{0x00, 0, 0, 0}); err == nil {
		t.Fatal("expected error for short message")
	}
	if _, _, err := registry.DecodeWire([]byte{0x01, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected error for bad magic byte")
	}
	if _, _, err := registry.DecodeWire([]byte{0x00, 0, 0, 0, 99}); err == nil {
		t.Fatal("expected error for unknown schema ID")
	}
}

func TestSchemaRegistryRegisterDuplicate(t *testing.T) {
	r := NewSchemaRegistry()
	c, err := NewCodec(`"int"`)
	if err != nil {
		t.Fatalf("NewCodec: %s", err)
	}
	if err := r.Register(5, c); err != nil {
		t.Fatalf("first Register: %s", err)
	}
	if err := r.Register(5, c); err == nil {
		t.Fatal("expected error on duplicate registration")
	}
	if err := r.Register(6, nil); err == nil {
		t.Fatal("expected error on nil codec registration")
	}
}
