package mysql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jinzhu/inflection"
	"github.com/kyleconroy/sqlc/internal/dinosql"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Result holds the mysql validated queries schema
type Result struct {
	Queries []*Query
	Schema  *Schema
	Config  *dinosql.Config
}

// GetConfig gives generator functions access to underlying settings and configuration
func (r *Result) GetConfig() *dinosql.Config {
	return r.Config
}

// Imports generates an import map
// TODO: implement
func (r *Result) Imports(packageSettings dinosql.PackageSettings) func(string) [][]string {
	lookup := func(v string) [][]string {
		var cols [][]string
		return cols
	}
	return lookup
}

// Enums generates parser-agnostic GoEnum types
// TODO: implement
func (r *Result) Enums() []dinosql.GoEnum {
	var enums []dinosql.GoEnum
	return enums
}

// Structs marshels each query into a go struct for generation
func (r *Result) Structs() []dinosql.GoStruct {
	var structs []dinosql.GoStruct
	for tableName, cols := range r.Schema.tables {
		s := dinosql.GoStruct{
			Name: inflection.Singular(dinosql.StructName(r, tableName)),
		}

		for _, col := range cols {
			s.Fields = append(s.Fields, dinosql.GoField{
				Name:    dinosql.StructName(r, col.Name.String()),
				Type:    goTypeCol(&col.Type),
				Tags:    map[string]string{"json": col.Name.String()},
				Comment: "",
			})
		}
		structs = append(structs, s)
	}

	return structs
}

// GoQueries generates parser-agnostic query information for code generation
func (r *Result) GoQueries() []dinosql.GoQuery {
	structs := r.Structs()

	qs := make([]dinosql.GoQuery, 0, len(r.Queries))
	for ix, query := range r.Queries {
		if query == nil {
			panic(fmt.Sprintf("query is nil on index: %v, len: %v", ix, len(r.Queries)))
		}
		if query.Name == "" {
			continue
		}
		if query.Cmd == "" {
			continue
		}

		gq := dinosql.GoQuery{
			Cmd:          query.Cmd,
			ConstantName: dinosql.LowerTitle(query.Name),
			FieldName:    dinosql.LowerTitle(query.Name) + "Stmt",
			MethodName:   query.Name,
			SourceName:   "queries", // query.Filename,
			SQL:          query.SQL,
			// Comments:     query.Comments,
		}

		if len(query.Params) == 1 {
			p := query.Params[0]
			gq.Arg = dinosql.GoQueryValue{
				Name: p.Name(),
				Typ:  p.GoType(),
			}
		} else if len(query.Params) > 1 {

			// needs conversion into a slice of interaces
			// although this is dirty, it is needed for the current implementation of columnsToStruct
			// where the first param uses the Structable interface
			structableSlice := make([]Structable, len(query.Params))
			for i := range query.Params {
				structableSlice[i] = query.Params[i]
			}

			gq.Arg = dinosql.GoQueryValue{
				Emit:   true,
				Name:   "arg",
				Struct: r.columnsToStruct(gq.MethodName+"Params", structableSlice),
			}
		}

		if len(query.Columns) == 1 {
			c := query.Columns[0]
			gq.Ret = dinosql.GoQueryValue{
				Name: columnName(c, 0),
				Typ:  goTypeCol(&c.Type),
			}
		} else if len(query.Columns) > 1 {
			var gs *dinosql.GoStruct
			var emit bool

			for _, s := range structs {
				if len(s.Fields) != len(query.Columns) {
					continue
				}
				same := true
				for i, f := range s.Fields {
					c := query.Columns[i]
					sameName := f.Name == dinosql.StructName(r, columnName(c, i))
					sameType := f.Type == goTypeCol(&c.Type)
					// TODO: consider making this deep equality from stdlib?
					// sameFQN := s.Table.EqualTo(&c.Table)
					if !sameName || !sameType || true { // !sameFQN
						same = false
					}
				}
				if same {
					gs = &s
					break
				}
			}

			if gs == nil {
				// needs conversion into a slice of interaces
				// although this is dirty, it is needed for the current implementation of columnsToStruct
				// where the first param uses the Structable interface
				structableSlice := make([]Structable, len(query.Columns))
				for i := range query.Columns {
					structableSlice[i] = columnDfnAlias(*query.Columns[i])
				}
				gs = r.columnsToStruct(gq.MethodName+"Row", structableSlice)
				emit = true
			}
			gq.Ret = dinosql.GoQueryValue{
				Emit:   emit,
				Name:   "i",
				Struct: gs,
			}
		}

		qs = append(qs, gq)
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].MethodName < qs[j].MethodName })
	return qs
}

type Structable interface {
	OriginalString() string
	GoType() string
}
type columnDfnAlias sqlparser.ColumnDefinition

func (col columnDfnAlias) GoType() string {
	return goTypeCol(&col.Type)
}
func (col columnDfnAlias) OriginalString() string {
	return col.Name.String()
}

func (r *Result) columnsToStruct(name string, items []Structable) *dinosql.GoStruct {
	gs := dinosql.GoStruct{
		Name: name,
	}
	seen := map[string]int{}
	for _, item := range items {
		name := item.OriginalString()
		typ := item.GoType()
		tagName := name
		fieldName := dinosql.StructName(r, name)
		if v := seen[name]; v > 0 {
			tagName = fmt.Sprintf("%s_%d", tagName, v+1)
			fieldName = fmt.Sprintf("%s_%d", fieldName, v+1)
		}
		gs.Fields = append(gs.Fields, dinosql.GoField{
			Name: fieldName,
			Type: typ,
			Tags: map[string]string{"json:": tagName},
		})
		seen[name]++
	}
	return &gs
}

func goTypeCol(col *sqlparser.ColumnType) string {
	switch t := col.Type; {
	case "varchar" == t:
		if col.NotNull {
			return "string"
		}
		return "sql.NullString"
	case "int" == t:
		if col.NotNull {
			return "int"
		}
		return "sql.NullInt64"
	case "float" == t, strings.HasPrefix(strings.ToLower(t), "decimal"):
		if col.NotNull {
			return "float64"
		}
		return "sql.NullFloat64"
	default:
		// TODO: remove panic here
		panic(fmt.Sprintf("Handle this col type directly: %v\n", col.Type))
		// return col.Type
	}
}

func columnName(c *sqlparser.ColumnDefinition, pos int) string {
	if !c.Name.IsEmpty() {
		return c.Name.String()
	}
	return fmt.Sprintf("column_%d", pos+1)
}

func argName(name string) string {
	out := ""
	for i, p := range strings.Split(name, "_") {
		if i == 0 {
			out += strings.ToLower(p)
		} else if p == "id" {
			out += "ID"
		} else {
			out += strings.Title(p)
		}
	}
	return out
}
