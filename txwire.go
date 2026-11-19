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
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire format magic byte used by the Confluent wire format:
//
//	0x00 | schemaID (4 bytes, big-endian) | avro binary payload
const wireMagicByte byte = 0x00

// Well known schema IDs for the two transaction fragment record types.  An
// application may register its own IDs via SchemaRegistry.Register when the
// producer uses different values.
const (
	// TxDataSchemaID is the default schema ID for TxData fragments.
	TxDataSchemaID int32 = 1
	// TxCommitSchemaID is the default schema ID for TxCommit fragments.
	TxCommitSchemaID int32 = 2
)

// TxDataSchema is the Avro schema for one transaction data fragment.
//
// A data fragment carries one row change (I/U/D) of one table inside one
// transaction.  Fragments of the same transaction are distinguished by their
// zero-based slot number and may be received interleaved, duplicated, or
// before the corresponding commit fragment.
//
// The "row" map values use the union ["null","boolean","int","long",
// "float","double","string","bytes","TxDecimal"] so that every column type
// Avro can represent round-trips without losing NULL/0/false or decimal
// precision.
const TxDataSchema = `{
  "type": "record",
  "name": "TxData",
  "namespace": "com.linkedin.goavro.tx",
  "fields": [
    {"name": "kind", "type": {"type": "enum", "name": "TxKind", "symbols": ["DATA", "COMMIT"]}},
    {"name": "txID", "type": "long"},
    {"name": "slot", "type": "int"},
    {"name": "table", "type": "string"},
    {"name": "op", "type": {"type": "enum", "name": "TxOp", "symbols": ["I", "U", "D"]}},
    {"name": "row", "type": {"type": "map", "values": ["null", "boolean", "int", "long", "float", "double", "string", "bytes", {"type": "record", "name": "TxDecimal", "fields": [
      {"name": "unscaled", "type": "bytes"},
      {"name": "scale", "type": "int"}
    ]}]}}
  ]
}`

// TxCommitSchema is the Avro schema for a transaction commit fragment.  It
// carries the continuous, gap-free commit sequence number assigned to the
// transaction and the total number of data slots the transaction contains.
// An empty transaction has slotCount 0 and may advance the applied head
// without any data fragments.
const TxCommitSchema = `{
  "type": "record",
  "name": "TxCommit",
  "namespace": "com.linkedin.goavro.tx",
  "fields": [
    {"name": "kind", "type": {"type": "enum", "name": "TxKind", "symbols": ["DATA", "COMMIT"]}},
    {"name": "txID", "type": "long"},
    {"name": "commitSeq", "type": "long"},
    {"name": "slotCount", "type": "int"}
  ]
}`

// SchemaRegistry maps the 4-byte schema ID carried by every wire message to
// the Codec able to decode its Avro payload.  A registry is safe for
// concurrent use after it has been populated.
type SchemaRegistry struct {
	codecs map[int32]*Codec
}

// NewSchemaRegistry returns an empty schema registry.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{codecs: make(map[int32]*Codec)}
}

// NewDefaultSchemaRegistry builds a registry pre-populated with Codecs for
// TxDataSchema and TxCommitSchema under their default schema IDs.
func NewDefaultSchemaRegistry() (*SchemaRegistry, error) {
	dataCodec, err := NewCodec(TxDataSchema)
	if err != nil {
		return nil, fmt.Errorf("cannot build TxData codec: %w", err)
	}
	commitCodec, err := NewCodec(TxCommitSchema)
	if err != nil {
		return nil, fmt.Errorf("cannot build TxCommit codec: %w", err)
	}
	r := NewSchemaRegistry()
	if err := r.Register(TxDataSchemaID, dataCodec); err != nil {
		return nil, err
	}
	if err := r.Register(TxCommitSchemaID, commitCodec); err != nil {
		return nil, err
	}
	return r, nil
}

// Register associates a schema ID with a Codec.  Registering the same ID
// twice is an error to catch producer/consumer configuration drift.
func (r *SchemaRegistry) Register(id int32, codec *Codec) error {
	if codec == nil {
		return errors.New("cannot register nil codec")
	}
	if _, exists := r.codecs[id]; exists {
		return fmt.Errorf("schema ID %d is already registered", id)
	}
	r.codecs[id] = codec
	return nil
}

// Codec returns the Codec registered under id, or an error when no codec is
// known for that schema ID.
func (r *SchemaRegistry) Codec(id int32) (*Codec, error) {
	c, ok := r.codecs[id]
	if !ok {
		return nil, fmt.Errorf("no codec registered for schema ID %d", id)
	}
	return c, nil
}

// EncodeWire prefixes the Avro binary datum with the Confluent style wire
// header (magic byte 0x00 and the big-endian schema ID) and appends the
// result to buf.
func (c *Codec) EncodeWire(buf []byte, schemaID int32, datum interface{}) ([]byte, error) {
	buf = append(buf, wireMagicByte)
	var idBytes [4]byte
	binary.BigEndian.PutUint32(idBytes[:], uint32(schemaID))
	buf = append(buf, idBytes[:]...)
	return c.BinaryFromNative(buf, datum)
}

// DecodeWire reads the Confluent style wire header from buf, looks up the
// Codec for the embedded schema ID in registry, and decodes the remaining
// Avro binary payload.  It returns the decoded datum and the schema ID so
// callers can distinguish TxData from TxCommit fragments.
func (r *SchemaRegistry) DecodeWire(buf []byte) (interface{}, int32, error) {
	if len(buf) < 5 {
		return nil, 0, errors.New("wire message too short: need at least 5 header bytes")
	}
	if buf[0] != wireMagicByte {
		return nil, 0, fmt.Errorf("invalid wire magic byte: expected 0x%02x, got 0x%02x", wireMagicByte, buf[0])
	}
	schemaID := int32(binary.BigEndian.Uint32(buf[1:5]))
	codec, err := r.Codec(schemaID)
	if err != nil {
		return nil, schemaID, err
	}
	datum, remaining, err := codec.NativeFromBinary(buf[5:])
	if err != nil {
		return nil, schemaID, fmt.Errorf("cannot decode payload for schema ID %d: %w", schemaID, err)
	}
	if len(remaining) != 0 {
		return nil, schemaID, fmt.Errorf("trailing %d byte(s) after Avro payload for schema ID %d", len(remaining), schemaID)
	}
	return datum, schemaID, nil
}
