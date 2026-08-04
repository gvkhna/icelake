package schemamap

import "github.com/gvkhna/icelake/internal/errdef"

// MaxNameLength is the longest a namespace or table name may be, in bytes. It
// matches the identifier limit most SQL engines that will query these tables
// already impose, and keeps the composed object key comfortably short.
const MaxNameLength = 63

// ValidateName reports whether value is usable as a namespace or table name,
// returning an [errdef.NameError] naming the offending value if it is not.
//
// The rule is ^[a-z0-9][a-z0-9_]*$, at most [MaxNameLength] bytes: lowercase
// letters, digits and underscores only, starting with a letter or digit. Each
// exclusion is mechanical rather than aesthetic. Slashes and dots break the
// two-segment <namespace>/<table> key layout that catalog rebuild discovers
// tables from. Case is excluded because object keys are case-sensitive while
// much of what surrounds them is not, so allowing both "Fills" and "fills"
// creates two tables that look like one. Hyphens are excluded because they are
// not valid unquoted SQL identifiers in most engines that will read these
// tables.
//
// A bad name is rejected, never normalized: silently rewriting a caller's
// table name is how two services end up sharing a table.
//
// The catalog-rebuild tool applies this same rule in reverse when parsing
// discovered prefixes, which is why it is exported rather than folded into
// [Declare].
func ValidateName(kind errdef.NameKind, value string) error {
	bad := errdef.NameError{Kind: kind, Value: value}

	if value == "" || len(value) > MaxNameLength {
		return bad
	}
	// Byte-wise on purpose: every byte of a multi-byte UTF-8 sequence is
	// >= 0x80 and so falls through to the default, which is the intended
	// rejection.
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_' && i > 0:
		default:
			return bad
		}
	}
	return nil
}
