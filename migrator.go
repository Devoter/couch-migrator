package migrator

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/go-kivik/kivik/v3"

	"github.com/Devoter/couch-migrator/migration"
)

// Migrator declares MongoDB migrations manager.
type Migrator struct {
	client     *kivik.Client
	migrations []migration.Migration
}

// NewMigrator returns a new instance of `Migrator`.
func NewMigrator(client *kivik.Client, migrations []migration.Migration) *Migrator {
	all := append(migrations, migration.Migration{Name: "-", Up: migration.DummyUpDown, Down: migration.DummyUpDown})
	sort.Sort(migration.Migrations(all))

	return &Migrator{
		client:     client,
		migrations: all,
	}
}

// Run interprets commands.
func (m *Migrator) Run(dbPrefix string, args ...string) (oldVersion int64, newVersion int64, err error) {
	if len(args) == 0 {
		err = ErrorCommandRequired
		return
	}

	switch args[0] {
	case "init":
		return m.Init(m.client, dbPrefix)
	case "up":
		var target int64

		target, err = m.parseVersion(false, args[1:]...)
		if err != nil {
			return
		}

		return m.Up(m.client, dbPrefix, target)
	case "down":
		return m.Down(m.client, dbPrefix)
	case "reset":
		return m.Reset(m.client, dbPrefix)
	case "version":
		return m.Version(m.client, dbPrefix)
	case "set_version":
		var target int64

		target, err = m.parseVersion(true, args[1:]...)
		if err != nil {
			return
		}

		return m.SetVersion(m.client, dbPrefix, target)
	default:
		err = ErrorUnexpectedCommand
		return
	}
}

// Init creates `migrations` collection if it does not exist and records the initial zero-migration.
func (m *Migrator) Init(client *kivik.Client, dbPrefix string) (oldVersion int64, newVersion int64, err error) {
	migr := &migration.Migration{Name: "-"}

	if err = client.CreateDB(context.TODO(), dbPrefix+"_migrations"); err != nil {
		switch kivik.StatusCode(err) {
		case http.StatusPreconditionFailed:
			err = ErrorMigrationsDatabaseAlreadyExists
		}

		if err != ErrorMigrationsDatabaseAlreadyExists {
			return
		}
	}

	shouldCreateIndex := err == nil

	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	if shouldCreateIndex {
		index := map[string]interface{}{"fields": []interface{}{"version"}}
		if err = db.CreateIndex(context.TODO(), "", "", index); err != nil {
			return
		}
	}

	var rows *kivik.Rows
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"version": 0,
		},
		"limit": 1,
	}

	rows, err = db.Find(context.TODO(), query)
	if err != nil {
		return
	} else if err = rows.Err(); err != nil {
		return
	}

	for rows.Next() {
		var m migration.Migration

		if err = rows.ScanDoc(&m); err != nil {
			break
		} else {
			err = ErrorMigrationsDatabaseAlreadyExists
			break
		}
	}

	rows.Close()

	if err != nil {
		return
	}

	_, _, err = db.CreateDoc(context.TODO(), migr)

	return
}

// Up upgrades database revision to the target or next version.
func (m *Migrator) Up(client *kivik.Client, dbPrefix string, target int64) (oldVersion int64, newVersion int64, err error) {
	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"version": map[string]interface{}{
				"$gt": -1,
			},
		},
		"sort": []interface{}{
			map[string]interface{}{"version": "asc"},
		},
	}

	history := []migration.Migration{}

	filler := func(item *migration.Migration) bool {
		item.Stored = true
		history = append(history, *item)

		return true
	}

	if err = m.findManyDocuments(db, filler, query); err != nil {
		return
	}

	length := len(history)

	if length > 0 {
		version := history[length-1].Version
		oldVersion = version
		newVersion = version
	}

	merged := m.mergeMigrations(history, m.migrations, target)

	for _, migr := range merged {
		if !migr.Stored {
			newVersion = migr.Version

			if err = migr.Up(client, dbPrefix); err != nil {
				return
			}

			migr.Stored = true

			if _, _, err = db.CreateDoc(context.TODO(), &migr); err != nil {
				return
			}
		}
	}

	return
}

// Down downgrades database revision to the previous version.
func (m *Migrator) Down(client *kivik.Client, dbPrefix string) (oldVersion int64, newVersion int64, err error) {
	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"version": map[string]interface{}{
				"$gt": -1,
			},
		},
		"sort": []interface{}{
			map[string]interface{}{"version": "desc"},
		},
		"limit": 1,
	}
	var old *migration.Migration

	old, err = m.findOneDocument(db, query)
	if err != nil {
		return
	}

	oldVersion = old.Version
	newVersion = old.Version

	for i := len(m.migrations) - 1; i >= 0; i-- {
		mig := m.migrations[i]

		if mig.Version == old.Version {
			if i > 0 {
				newVersion = m.migrations[i-1].Version

				if err = mig.Down(client, dbPrefix); err != nil {
					return
				}

				if err = m.purgeDocument(db, old.ID); err != nil {
					return
				}
			}

			return
		}
	}

	return
}

// Reset resets database to the zero-revision.
func (m *Migrator) Reset(client *kivik.Client, dbPrefix string) (oldVersion int64, newVersion int64, err error) {
	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"version": map[string]interface{}{
				"$gt": -1,
			},
		},
		"sort": []interface{}{
			map[string]interface{}{"version": "asc"},
		},
	}

	history := []migration.Migration{}

	filler := func(item *migration.Migration) bool {
		item.Stored = true
		history = append(history, *item)

		return true
	}

	if err = m.findManyDocuments(db, filler, query); err != nil {
		return
	}

	length := len(history)

	if length > 0 {
		version := history[length-1].Version
		oldVersion = version
		newVersion = version
	} else {
		return
	}

	correlated, err := m.correlateMigrations(history, m.migrations)
	if err != nil {
		return
	}

	for i := len(correlated) - 1; i >= 0; i-- {
		migr := correlated[i]

		if i > 0 {
			newVersion = correlated[i-1].Version
		} else {
			newVersion = migr.Version
		}

		if err = migr.Down(client, dbPrefix); err != nil {
			return
		}

		migr.Stored = true

		// don't delete zero migration
		if migr.Version > 0 {
			var mig *migration.Migration

			query := map[string]interface{}{
				"selector": map[string]interface{}{
					"version": migr.Version,
				},
			}

			mig, err = m.findOneDocument(db, query)
			if err != nil {
				return
			}

			if err = m.purgeDocument(db, mig.ID); err != nil {
				return
			}
		}
	}

	return
}

// Version returns current database revision version.
func (m *Migrator) Version(client *kivik.Client, dbPrefix string) (oldVersion int64, newVersion int64, err error) {
	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"version": map[string]interface{}{
				"$gt": -1,
			},
		},
		"sort": []interface{}{
			map[string]interface{}{"version": "desc"},
		},
	}

	var mig *migration.Migration

	mig, err = m.findOneDocument(db, query)
	if err != nil {
		return
	}

	oldVersion = mig.Version
	newVersion = mig.Version
	return
}

// SetVersion forces database revisiton version.
func (m *Migrator) SetVersion(client *kivik.Client, dbPrefix string, target int64) (oldVersion int64,
	newVersion int64, err error) {
	oldVersion, _, err = m.Version(client, dbPrefix)
	if err != nil {
		return
	}

	index := -1
	migs := make([]interface{}, 0, len(m.migrations))

	for i, migr := range m.migrations {
		migs = append(migs, migr)
		if migr.Version == target {
			index = i
			break
		}
	}

	if index == -1 {
		err = ErrorTargetVersionNotFound
		return
	} else if oldVersion == m.migrations[index].Version {
		newVersion = oldVersion
		return
	}

	db := client.DB(context.TODO(), dbPrefix+"_migrations")
	if err = db.Err(); err != nil {
		return
	}

	history := []migration.Migration{}

	filler := func(item *migration.Migration) bool {
		item.Stored = true
		history = append(history, *item)

		return true
	}

	if err = m.findManyDocuments(db, filler, map[string]interface{}{"selector": map[string]interface{}{}}); err != nil {
		return
	}

	for _, his := range history {
		if err = m.purgeDocument(db, his.ID); err != nil {
			return
		}
	}

	if _, err = db.BulkDocs(context.TODO(), migs); err != nil {
		return
	}

	newVersion = migs[len(migs)-1].(migration.Migration).Version
	return
}

func (m *Migrator) parseVersion(required bool, args ...string) (version int64, err error) {
	if len(args) == 0 {
		if required {
			err = ErrorVersionNumberRequired
			return
		}

		version = -1
		return
	}

	version, err = strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		err = ErrorInvalidVersionArgumentFormat
		return
	}

	return
}

// mergreMigrations returns a slice contains a sorted list of all migrations (applied and actual).
func (m *Migrator) mergeMigrations(applied, actual []migration.Migration, target int64) []migration.Migration {
	appliedLength := len(applied)
	actualLength := len(actual)
	merged := make([]migration.Migration, 0, appliedLength+actualLength)
	i := 0
	j := 0
	var max int64

	if actualLength > 0 {
		if target == -1 {
			max = actual[actualLength-1].Version + 1
		} else {
			max = target + 1
		}
	}

	for (i < appliedLength) && (j < actualLength) && (actualLength == 0 || actual[j].Version < max) {
		if applied[i].Less(&actual[j]) {
			merged = append(merged, applied[i])
			i++
		} else if actual[j].Less(&applied[j]) {
			merged = append(merged, actual[j])
			j++
		} else {
			merged = append(merged, applied[i])
			i++
			j++
		}
	}

	for i < appliedLength {
		merged = append(merged, applied[i])
		i++
	}

	for j < actualLength && (actualLength == 0 || actual[j].Version < max) {
		merged = append(merged, actual[j])
		j++
	}

	return merged
}

// CorrelateMigrations returns a list of correlated migrations.
// This method replaces stored migrations with actual migrations. If some actual migration is absent
// the method returns an error and a list which contains missing migration as the last item.
func (m *Migrator) correlateMigrations(applied, actual []migration.Migration) (correlated []migration.Migration, err error) {
	appliedLength := len(applied)
	actualLength := len(actual)
	i := 0
	j := 0
	correlated = make([]migration.Migration, 0, appliedLength)

	for (i < appliedLength) && (j < actualLength) {
		if applied[i].Less(&actual[j]) {
			correlated = append(correlated, applied[i])
			err = ErrorSomeMigrationsAreAbsent
			return
		} else if actual[j].Less(&applied[i]) {
			// skip unapplied migrations
			j++
		} else {
			correlated = append(correlated, actual[j])
			i++
			j++
		}
	}

	if i < appliedLength {
		correlated = append(correlated, applied[i])
		err = ErrorSomeMigrationsAreAbsent
	}

	return
}

func (m *Migrator) findManyDocuments(db *kivik.DB, filler func(*migration.Migration) bool,
	query interface{}, options ...kivik.Options) error {
	rows, err := db.Find(context.TODO(), query, options...)
	if err != nil {
		// ToDo: check no documents
		return err
	} else if err = rows.Err(); err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var mig migration.Migration

		if err := rows.ScanDoc(&mig); err != nil {
			return err
		}

		if !filler(&mig) {
			return nil
		}
	}

	return nil
}

func (m *Migrator) findOneDocument(db *kivik.DB, query interface{}, options ...kivik.Options) (*migration.Migration, error) {
	rows, err := db.Find(context.TODO(), query, options...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	if rows.Next() {
		var mig migration.Migration

		if err := rows.ScanDoc(&mig); err != nil {
			return nil, err
		}

		return &mig, nil
	}

	return nil, ErrorNoMigrations
}

func (m *Migrator) purgeDocument(db *kivik.DB, id string) (err error) {
	row := db.Get(context.TODO(), id, map[string]interface{}{"revs": true})
	if err = row.Err; err != nil {
		return err
	}

	var revs revisions

	if err := row.ScanDoc(&revs); err != nil {
		return err
	}

	req := map[string][]string{id: {}}

	for i := 0; i < len(revs.Revisions.IDs); i++ {
		req[id] = append(req[id], fmt.Sprintf("%d-%s", revs.Revisions.Start-i, revs.Revisions.IDs[i]))
	}

	_, err = db.Purge(context.TODO(), req)

	return err
}

type revisions struct {
	ID        string        `json:"_id"`
	Rev       string        `json:"_rev"`
	Revisions revisionsList `json:"_revisions"`
}

type revisionsList struct {
	IDs   []string `json:"ids"`
	Start int      `json:"start"`
}
