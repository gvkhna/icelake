// Package metascan decides which of a table's metadata files is current, from
// their names alone.
//
// The rule lives here, in one place, because two packages apply it and must
// never disagree: catalog rebuild (the rebuild package) registers the file this
// ordering picks, and inspection (the inspect package) reports a table's state
// from the same choice. The full form the SQL catalog writes, verified against
// a real table, is NNNNN-<uuid>.metadata.json: a zero-padded sequence that
// starts at 00000 and increments on every commit, then a fresh UUID. No
// version-hint.text is written on this path, so nothing here looks for one —
// the sequence in the name is the only ordering that exists.
package metascan

import (
	"path"
	"sort"
	"strconv"
	"strings"
)

// Suffix is the tail every Iceberg table metadata file's name carries.
const Suffix = ".metadata.json"

// Candidate is one metadata file, with the sequence number parsed out of its
// name.
type Candidate struct {
	// Key is the object key exactly as it was listed.
	Key string
	// Sequence is the commit count parsed from the name's leading digits.
	Sequence int
}

// Candidates reduces a metadata directory's listing to the metadata files it
// holds, highest sequence first.
//
// Everything else in that directory belongs to the table too — the manifest
// lists and manifests iceberg-go writes beside the metadata files — so those
// are filtered by name rather than reported as junk. Only names matching the
// documented NNNNN-... .metadata.json form survive, and the sequence is parsed
// with a plain integer conversion rather than a fixed five-digit assumption,
// because the padding is a formatting choice and the hundred-thousandth commit
// is not a failure.
func Candidates(keys []string) []Candidate {
	var out []Candidate
	for _, key := range keys {
		name := path.Base(key)
		if !strings.HasSuffix(name, Suffix) {
			continue
		}
		dash := strings.IndexByte(name, '-')
		if dash <= 0 {
			continue
		}
		sequence, err := strconv.Atoi(name[:dash])
		if err != nil || sequence < 0 {
			continue
		}
		out = append(out, Candidate{Key: key, Sequence: sequence})
	}

	// Highest sequence first, and the key breaks a tie so that two files
	// claiming one sequence produce the same answer on every run rather than
	// whichever the listing happened to return first.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence > out[j].Sequence
		}

		return out[i].Key > out[j].Key
	})

	return out
}
