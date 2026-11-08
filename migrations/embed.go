// Package migrations embeds the SQL migrations. internal/migrate applies them in
// version order (design §3). Files are named NNNN_name.sql and never edited once
// merged: a change to the schema is a new file.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
