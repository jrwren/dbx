// Copyright (C) 2017 Space Monkey, Inc.

package main

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"gopkg.in/spacemonkeygo/dbx.v1/testutil"
)

func TestCompilation(t *testing.T) {
	tw := testutil.Wrap(t)
	tw.Parallel()

	data_dir := filepath.Join("testdata", "build")

	fileinfos, err := ioutil.ReadDir(data_dir)
	tw.AssertNoError(err)

	for _, fileinfo := range fileinfos {
		fileinfo := fileinfo
		tw.Runp(fileinfo.Name(), func(tw *testutil.T) {
			path := filepath.Join(data_dir, fileinfo.Name())
			testFile(tw, path)
		})
	}
}

func testFile(t *testutil.T, file string) {
	defer func() {
		if val := recover(); val != nil {
			t.Fatalf("%s\n%s", val, string(debug.Stack()))
		}
	}()

	dir, err := ioutil.TempDir("", "dbx")
	t.AssertNoError(err)
	defer os.RemoveAll(dir)

	dbx_source, err := ioutil.ReadFile(file)
	t.AssertNoError(err)
	t.Context("dbx", linedSource(dbx_source))
	d := loadDirectives(t, dbx_source)

	dialects := []string{"postgres", "sqlite3", "spanner"}
	if other := d.lookup("dialects"); other != nil {
		dialects = other
		t.Logf("using dialects: %q", dialects)
	}

	runBuild := func(rx, userdata bool) {
		t.Logf("[%s] generating... (rx=%t, userdata=%t)", file, rx, userdata)
		err = golangCmd("", dialects, "", rx, userdata, file, dir)
		t.AssertNoError(err)

		t.Logf("[%s] loading...", file)
		go_file := filepath.Join(dir, filepath.Base(file)+".go")
		go_source, err := ioutil.ReadFile(go_file)
		t.AssertNoError(err)
		t.Context("go", linedSource(go_source))

		t.Logf("[%s] parsing...", file)
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, go_file, go_source, parser.AllErrors)
		t.AssertNoError(err)

		t.Logf("[%s] compiling...", file)
		config := types.Config{
			Importer: importer.Default(),
		}
		_, err = config.Check(dir, fset, []*ast.File{f}, nil)

		if d.has("fail") {
			t.AssertError(err, d.get("fail"))
		} else {
			t.AssertNoError(err)
		}
	}

	runBuild(false, false)
	runBuild(false, true)
	runBuild(true, false)
	runBuild(true, true)
}
