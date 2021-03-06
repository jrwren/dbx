// Copyright (C) 2017 Space Monkey, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// DBX implements code generation for database schemas and accessors.
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	cli "github.com/jawher/mow.cli"
	"github.com/spacemonkeygo/errors"
	"gopkg.in/spacemonkeygo/dbx.v1/code/golang"
	"gopkg.in/spacemonkeygo/dbx.v1/errutil"
	"gopkg.in/spacemonkeygo/dbx.v1/ir"
	"gopkg.in/spacemonkeygo/dbx.v1/ir/xform"
	"gopkg.in/spacemonkeygo/dbx.v1/sql"
	"gopkg.in/spacemonkeygo/dbx.v1/sqlgen"
	"gopkg.in/spacemonkeygo/dbx.v1/syntax"
	"gopkg.in/spacemonkeygo/dbx.v1/templates"
	"gopkg.in/spacemonkeygo/dbx.v1/tmplutil"
)

// dbx golang (-p package) (-d dialect) DBXFILE OUTDIR
// dbx schema (-d dialect) DBXFILE OUTDIR

var loadedData []byte

func main() {
	die := func(err error) {
		if err != nil {
			// if the error came from errutil, don't bother with the dbx prefix
			err_string := strings.TrimPrefix(errors.GetMessage(err), "dbx: ")
			fmt.Fprintln(os.Stderr, err_string)
			if context := errutil.GetContext(loadedData, err); context != "" {
				fmt.Fprintln(os.Stderr)
				fmt.Fprintln(os.Stderr, "context:")
				fmt.Fprintln(os.Stderr, context)
			}
			cli.Exit(1)
		}
	}

	app := cli.App("dbx", "generate SQL schema and matching code")

	app.Command("golang", "generate Go code", func(cmd *cli.Cmd) {
		package_opt := cmd.StringOpt("p package", "",
			"package name for generated code")
		dialects_opt := cmd.StringsOpt("d dialect", nil,
			"SQL dialects (defaults to postgres)")
		templatedir_opt := cmd.StringOpt("t templates", "",
			"override the template directory")
		rx_opt := cmd.BoolOpt("rx", true,
			"generate Rx support")
		userdata_opt := cmd.BoolOpt("userdata", false,
			"generate userdata interface and mutex on models")
		dbxfile_arg := cmd.StringArg("DBXFILE", "",
			"path to dbx file")
		outdir_arg := cmd.StringArg("OUTDIR", "",
			"output directory")
		cmd.Action = func() {
			die(golangCmd(*package_opt, *dialects_opt, *templatedir_opt,
				*rx_opt, *userdata_opt, *dbxfile_arg, *outdir_arg))
		}
	})

	app.Command("schema", "generate table schema", func(cmd *cli.Cmd) {
		dialects_opt := cmd.StringsOpt("d dialect", nil,
			"SQL dialects (default is postgres)")
		dbxfile_arg := cmd.StringArg("DBXFILE", "",
			"path to dbx file")
		outdir_arg := cmd.StringArg("OUTDIR", "",
			"output directory")
		cmd.Action = func() {
			die(schemaCmd(*dialects_opt, *dbxfile_arg, *outdir_arg))
		}
	})

	app.Command("format", "format dbx file on stdin", func(cmd *cli.Cmd) {
		cmd.Action = func() { die(formatCmd()) }
	})

	die(app.Run(os.Args))
}

func golangCmd(pkg string, dialects_opt []string, template_dir string,
	rx bool, userdata bool, dbxfile, outdir string) (err error) {

	if pkg == "" {
		base := filepath.Base(dbxfile)
		pkg = base[:len(base)-len(filepath.Ext(base))]
	}

	fw := newFileWriter(outdir, dbxfile)

	root, err := parseDBX(dbxfile)
	if err != nil {
		return err
	}

	dialects, err := createDialects(dialects_opt)
	if err != nil {
		return err
	}

	loader := getLoader(template_dir)

	renderer, err := golang.New(loader, &golang.Options{
		Package:         pkg,
		SupportRx:       rx,
		SupportUserdata: userdata,
	})
	if err != nil {
		return err
	}

	rendered, err := renderer.RenderCode(root, dialects)
	if err != nil {
		return err
	}

	if err := fw.writeFile("go", rendered); err != nil {
		return err
	}

	return nil
}

func schemaCmd(dialects_opt []string, dbxfile, outdir string) (err error) {
	fw := newFileWriter(outdir, dbxfile)

	root, err := parseDBX(dbxfile)
	if err != nil {
		return err
	}

	dialects, err := createDialects(dialects_opt)
	if err != nil {
		return err
	}

	for _, dialect := range dialects {
		err = fw.writeFile(dialect.Name()+".sql", renderSchema(dialect, root))
		if err != nil {
			return err
		}
	}

	return nil
}

func formatCmd() (err error) {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	loadedData = data

	formatted, err := syntax.Format("", data)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(formatted)
	return err
}

func renderSchema(dialect sql.Dialect, root *ir.Root) []byte {
	const schema_hdr = `-- AUTOGENERATED BY gopkg.in/spacemonkeygo/dbx.v1
-- DO NOT EDIT`

	rendered := sqlgen.Render(dialect,
		sql.SchemaSQL(root, dialect),
		sqlgen.NoTerminate, sqlgen.NoFlatten)

	return []byte(schema_hdr + "\n" + rendered + "\n")
}

func parseDBX(in string) (*ir.Root, error) {
	data, err := ioutil.ReadFile(in)
	if err != nil {
		return nil, err
	}
	loadedData = data

	ast_root, err := syntax.Parse(in, data)
	if err != nil {
		return nil, err
	}

	return xform.Transform(ast_root)
}

func getLoader(dir string) tmplutil.Loader {
	loader := tmplutil.BinLoader(templates.Asset)
	if dir != "" {
		return tmplutil.DirLoader(dir, loader)
	}
	return loader
}

func createDialects(which []string) (out []sql.Dialect, err error) {
	if len(which) == 0 {
		which = append(which, "postgres")
	}
	for _, name := range which {
		var d sql.Dialect
		switch name {
		case "postgres":
			d = sql.Postgres()
		case "sqlite3":
			d = sql.SQLite3()
		case "spanner":
			d = sql.Spanner()
		default:
			return nil, fmt.Errorf("unknown dialect %q", name)
		}
		out = append(out, d)
	}
	return out, nil
}

type fileWriter struct {
	dir    string
	prefix string
}

func newFileWriter(outdir, dbxfile string) *fileWriter {
	return &fileWriter{
		dir:    outdir,
		prefix: filepath.Base(dbxfile),
	}
}

func (fw *fileWriter) writeFile(suffix string, data []byte) (err error) {
	file_path := filepath.Join(fw.dir, fw.prefix+"."+suffix)
	tmp_path := file_path + ".tmp"

	if err := ioutil.WriteFile(tmp_path, data, 0644); err != nil {
		return fmt.Errorf("unable to write %s: %v", tmp_path, err)
	}

	if err := os.Rename(tmp_path, file_path); err != nil {
		os.Remove(tmp_path)
		return fmt.Errorf("unable to rename %s over %s: %v", tmp_path,
			file_path, err)
	}

	return nil
}
