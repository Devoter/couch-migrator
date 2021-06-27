package migration

import "github.com/go-kivik/kivik/v3"

// ApplyFunc declares func type for migration functions
type ApplyFunc func(client *kivik.Client, dbPrefix string) error

// Migration declares a migration data structure.
type Migration struct {
	ID      string    `json:"_id,omitempty"`
	Version int64     `json:"version"`
	Name    string    `json:"name"`
	Up      ApplyFunc `json:"-"`
	Down    ApplyFunc `json:"-"`
	Stored  bool      `json:"-"`
}

// Less returns `true` if an argument is more than current.
func (mig *Migration) Less(migration *Migration) bool {
	return CompareMigrations(mig, migration)
}

// Eq returns `true` if migrations version are equal.
func (mig *Migration) Eq(migration *Migration) bool {
	return mig.Version == migration.Version
}

// Migrations type declares a slice-type of `Migration` with an implementation of `sort.Sort` interface.
type Migrations []Migration

func (ms Migrations) Len() int {
	return len(ms)
}

func (ms Migrations) Swap(i int, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Migrations) Less(i int, j int) bool {
	return CompareMigrations(&ms[i], &ms[j])
}

// CompareMigrations compares two migrations and returns `true` if `left` migration is less.
func CompareMigrations(left *Migration, right *Migration) bool {
	return left.Version < right.Version
}
