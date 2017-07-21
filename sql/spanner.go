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

package sql

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/spacemonkeygo/dbx.v1/consts"
	"gopkg.in/spacemonkeygo/dbx.v1/ir"
)

type spanner struct {
}

func Spanner() Dialect {
	return &spanner{}
}

func (p *spanner) Name() string {
	return "spanner"
}

func (p *spanner) Features() Features {
	return Features{
		Returning:           true,
		PositionalArguments: true,
		NoLimitToken:        "ALL",
	}
}

func (p *spanner) RowId() string {
	return ""
}

func (p *spanner) ColumnType(field *ir.Field) string {
	switch field.Type {
	case consts.SerialField:
		return "INT64"
	case consts.Serial64Field:
		return "INT64"
	case consts.IntField:
		return "INT64"
	case consts.Int64Field:
		return "INT64"
	case consts.UintField:
		return "INT64"
	case consts.Uint64Field:
		return "INT64"
	case consts.FloatField:
		return "FLOAT64"
	case consts.Float64Field:
		return "FLOAT64"
	case consts.TextField:
		if field.Length > 0 {
			return fmt.Sprintf("STRING(%d)", field.Length)
		}
		return "STRING(MAX)"
	case consts.BoolField:
		return "BOOL"
	case consts.TimestampField:
		return "TIMESTAMP"
	case consts.TimestampUTCField:
		return "TIMESTAMP"
	case consts.BlobField:
		return "BYTES(MAX)"
	case consts.DateField:
		return "DATE"
	default:
		panic(fmt.Sprintf("unhandled field type %s", field.Type))
	}
}

func (p *spanner) Rebind(sql string) string {
	out := make([]byte, 0, len(sql)+10)

	j := 1
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch != '?' {
			out = append(out, ch)
			continue
		}

		out = append(out, '$')
		out = append(out, strconv.Itoa(j)...)
		j++
	}

	return string(out)
}

func (p *spanner) ArgumentPrefix() string { return "$" }

var spannerEscaper = strings.NewReplacer(
	`'`, `\'`,
	`\`, `\\`,
)

func (p *spanner) EscapeString(s string) string {
	return spannerEscaper.Replace(s)
}
