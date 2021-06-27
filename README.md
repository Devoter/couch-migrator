# couch-migrator

Couch migrator is a library that provides migration management operations.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Contents

1. [Installation](#installation)
2. [Usage](#usage)
    1. [Migrations package](#migrations-package)
    2. [Global slice variable example](#global-slice-variable-example)
2. [License](#license)

## Installation

```sh
go get -v github.com/Devoter/couch-migrator
```

## Usage

The following instructions are only recommendations, of course, you can use the library as like as you wish.

### Migrations package

To use the library you should create migrations functions. It is intuitive to declare `migrations` package in your project. All your migrations should be placed in one slice of type `[]migration.Migration`.

#### Global slice variable example

There is a simple way to declare the migrations slice:

```go
// migrations/migrations.go
package migrations

import "github.com/Devoter/couch-migrator/migration"

// Migrations is a list of all available migrations.
var Migrations = []migration.Migration{}
```

## License

[MIT](LICENSE)
