package migration

import "github.com/go-kivik/kivik/v3"

// DummyUpDown is a dummy migration function.
func DummyUpDown(client *kivik.Client, dbPrefix string) error {
	return nil
}
