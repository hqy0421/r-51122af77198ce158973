// Package main demonstrates goavro transaction fragment loading: decoding
// schema-ID-prefixed Avro TxData/TxCommit fragments and applying complete
// transactions to SQLite strictly in commit order.
//
// Run with:
//
//	go run ./examples/txloader
package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"

	goavro "github.com/linkedin/goavro/v2"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	dsn := "file:txloader-demo.db?cache=shared"

	// 1. Target tables.  Foreign keys may be plain (non-DEFERRABLE): the
	// loader defers them per transaction so child rows can precede parents.
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = db.Exec(`DROP TABLE IF EXISTS child`)
	_, _ = db.Exec(`DROP TABLE IF EXISTS parent`)
	_, err = db.Exec(`
CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT, amount TEXT);
CREATE TABLE child  (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent(id), note TEXT);`)
	if err != nil {
		log.Fatal(err)
	}
	db.Close()

	// 2. Schema registry and loader with per-table SQL bindings.
	registry, err := goavro.NewDefaultSchemaRegistry()
	if err != nil {
		log.Fatal(err)
	}
	loader, err := goavro.NewFragmentLoader(goavro.FragmentLoaderConfig{
		DSN:      dsn,
		Registry: registry,
		Bindings: map[string]goavro.TableBinding{
			"parent": {Table: "parent", Columns: []string{"id", "label", "amount"}, PrimaryKey: []string{"id"}},
			"child":  {Table: "child", Columns: []string{"id", "parent_id", "note"}, PrimaryKey: []string{"id"}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer loader.Close()

	dataCodec, err := registry.Codec(goavro.TxDataSchemaID)
	if err != nil {
		log.Fatal(err)
	}
	commitCodec, err := registry.Codec(goavro.TxCommitSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Build one transaction whose slots arrive child-before-parent.
	childRow := map[string]interface{}{
		"kind": "DATA", "txID": int64(100), "slot": int32(0),
		"table": "child", "op": "I",
		"row": map[string]interface{}{
			"id":        map[string]interface{}{"long": int64(1)},
			"parent_id": map[string]interface{}{"long": int64(7)},
			"note":      nil, // NULL stays NULL
		},
	}
	unscaled, _ := new(big.Int).SetString("4020", 10)
	decimalBytes, err := goavroSigned(unscaled)
	if err != nil {
		log.Fatal(err)
	}
	parentRow := map[string]interface{}{
		"kind": "DATA", "txID": int64(100), "slot": int32(1),
		"table": "parent", "op": "I",
		"row": map[string]interface{}{
			"id":    map[string]interface{}{"long": int64(7)},
			"label": map[string]interface{}{"string": "demo"},
			"amount": map[string]interface{}{"com.linkedin.goavro.tx.TxDecimal": map[string]interface{}{
				"unscaled": decimalBytes, "scale": int32(2),
			}},
		},
	}
	commit := map[string]interface{}{
		"kind": "COMMIT", "txID": int64(100),
		"commitSeq": int64(1), "slotCount": int32(2),
	}

	for _, datum := range []map[string]interface{}{childRow, parentRow, commit} {
		var wire []byte
		if datum["kind"] == "COMMIT" {
			wire, err = commitCodec.EncodeWire(nil, goavro.TxCommitSchemaID, datum)
		} else {
			wire, err = dataCodec.EncodeWire(nil, goavro.TxDataSchemaID, datum)
		}
		if err != nil {
			log.Fatal(err)
		}
		result, err := loader.Ingest(wire)
		if err != nil {
			log.Fatal(err)
		}
		if result.PumpError != nil {
			log.Fatalf("application blocked: %v", result.PumpError)
		}
	}

	head, err := loader.AppliedHead()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("appliedHead=%d (receive=%d)\n", head, must(loader.ReceivePosition()))

	db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var label, amount string
	if err := db.QueryRow(`SELECT label, amount FROM parent WHERE id = 7`).Scan(&label, &amount); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("parent: label=%s amount=%s\n", label, amount)
	var note sql.NullString
	if err := db.QueryRow(`SELECT note FROM child WHERE id = 1`).Scan(&note); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("child: note.Valid=%v\n", note.Valid)
	// Output:
	// appliedHead=1 (receive=3)
	// parent: label=demo amount=40.20
	// child: note.Valid=false
}

func must(v int64, err error) int64 {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

// goavroSigned renders a big.Int as two's-complement big-endian bytes, the
// shape the TxDecimal.unscaled field expects.
func goavroSigned(n *big.Int) ([]byte, error) {
	// Add a high zero byte for non-negative values whose top bit is set, so
	// they are not misread as negative.
	b := n.Bytes()
	if n.Sign() >= 0 && len(b) > 0 && b[0]&0x80 != 0 {
		return append([]byte{0}, b...), nil
	}
	return b, nil
}
