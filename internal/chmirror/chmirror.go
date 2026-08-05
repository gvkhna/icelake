package chmirror

import (
	"crypto/tls"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/gvkhna/icelake/internal/errdef"
)

// DefaultDatabase is the ClickHouse database the mirror uses when none is
// configured.
//
// One database rather than one per namespace, which ARCHITECTURE.md records as
// a decision and argues on the operation that matters most for a disposable
// index: dropping it. One name is the whole of "throw the mirror away and let
// it be rebuilt", and one GRANT is the whole of the permission surface.
const DefaultDatabase = "icelake"

// Settings is everything this package needs to reach a server. It is the
// internal twin of the public ClickHouseConfig, validated by the time one of
// these exists.
type Settings struct {
	// Addr is the host:port of the server's native protocol port.
	Addr string
	// Database is the database the mirror's tables live in. Empty means
	// [DefaultDatabase]; the public configuration resolves it before this
	// package sees it, and this is the second line.
	Database string
	// Username is the user to connect as.
	Username string
	// Password is that user's password, or empty when the user has none.
	Password string
	// TLS says whether to connect over TLS.
	TLS bool
}

// Conn is the process-wide handle onto one ClickHouse server: one connection
// pool, shared by every mirrored table, exactly as the staging store and the
// object-storage client are shared.
type Conn struct {
	db       driver.Conn
	database string
}

// Open builds the handle. It deliberately makes no network call — the driver
// dials lazily, and a mirror that failed at [icelake.Open] would be ClickHouse
// deciding whether the lake starts, which is the one thing this design forbids.
// The first thing that actually reaches the server is [Table.Ensure], on the
// flush path or at OpenWriter, where a failure is a notification rather than a
// refusal.
func Open(s Settings) (*Conn, error) {
	database := s.Database
	if database == "" {
		database = DefaultDatabase
	}

	opts := &clickhouse.Options{
		Addr: []string{s.Addr},
		Auth: clickhouse.Auth{
			// Deliberately the *default* database on the connection rather than
			// the mirror's own: the mirror creates its database, and a
			// connection that could not be established until the database
			// existed could never create it.
			Database: "default",
			Username: s.Username,
			Password: s.Password,
		},
	}
	if s.TLS {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, errdef.NewClickHouseError(errdef.ClickHouseKindConnect, "", "", s.Addr,
			"building the client", err)
	}

	return &Conn{db: db, database: database}, nil
}

// Database returns the database the mirror's tables live in.
func (c *Conn) Database() string { return c.database }

// Close releases the connection pool. A failure to close is reported rather
// than swallowed, for the same reason a staging close failure is: it means work
// was served and the last of it is in doubt.
func (c *Conn) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	if err := c.db.Close(); err != nil {
		return errdef.NewClickHouseError(errdef.ClickHouseKindConnect, "", "", c.database,
			"closing the connection", err)
	}

	return nil
}

// quoteIdent renders a ClickHouse identifier: backtick-quoted, with backslashes
// and backticks escaped.
//
// It is applied to every identifier this package writes into a statement,
// including ones that came from a validated namespace or table name. Names
// reaching a column position have been through no such rule — a declaration
// struct's parquet tag and a schema document's field name are whatever the
// caller wrote — so quoting is the boundary rather than a formality, and
// applying it everywhere means there is no second rule to remember about which
// identifiers are trusted.
func quoteIdent(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('`')
	for i := range len(s) {
		switch c := s[i]; c {
		case '\\', '`':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('`')

	return b.String()
}
