// Package imageassets exposes embedded Kafkaesque brand images.
package imageassets

import "embed"

// Files contains the brand images served by the embedded UI.
//
//go:embed KafkaesqueLogo.png
var Files embed.FS
