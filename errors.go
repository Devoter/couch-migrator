package migrator

// Error declares constant error type.
type Error string

func (e Error) Error() string {
	return string(e)
}

// ErrorNoMigrations means that no migrations ware found at the specified path.
const ErrorNoMigrations = Error("No migrations")

// ErrorCommandRequired means that no command was specified.
const ErrorCommandRequired = Error("Command required")

// ErrorUnexpectedCommand means that command name is unknown.
const ErrorUnexpectedCommand = Error("Unexpected command")

// ErrorInvalidVersionArgumentFormat means that the format of an argument named `version` does not correspond to int64.
const ErrorInvalidVersionArgumentFormat = Error("Invalid version argument format")

// ErrorVersionNumberRequired means that no version number was specified via command line.
const ErrorVersionNumberRequired = Error("Version number required")

// ErrorMigrationsDatabaseAlreadyExists means that `migrations` database already exists.
const ErrorMigrationsDatabaseAlreadyExists = Error("Migrations database already exists")

// ErrorUnequalCountsOfMigrations means that the count of `up` migrations is not equal to the count of `down` migrations.
const ErrorUnequalCountsOfMigrations = Error("Unequal counts of `up` and `down` migrations")

// ErrorMigrationsAreNotInitialized means that migrations database is empty.
const ErrorMigrationsAreNotInitialized = Error("Migrations are not initialized, try to call `init` at first")

// ErrorTargetVersionNotFound means that the target migration version was not found in migrations list.
const ErrorTargetVersionNotFound = Error("Target migration version was not found")

// ErrorSomeMigrationsAreAbsent means that some migrations files are absent.
const ErrorSomeMigrationsAreAbsent = Error("Some migrations are absent")
