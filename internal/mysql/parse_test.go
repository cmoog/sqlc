package mysql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/kyleconroy/sqlc/internal/dinosql"
	"vitess.io/vitess/go/vt/sqlparser"
)

func init() {
	initMockSchema()
}

const query = `
/* name: GetAllStudents :many */
SELECT school_id, id FROM students WHERE id = :id + ?
`

const create = `
	CREATE TABLE students (
		id int,
		school_id VARCHAR(255),
		school_lat VARCHAR(255),
		PRIMARY KEY (ID)
	);`

const filename = "test.sql"

func TestParseFile(t *testing.T) {
	s := NewSchema()
	_, err := parseFile(filename, s)
	keep(err)
}

func TestGenerate(t *testing.T) {
	// t.Skip()
	s := NewSchema()
	result, _ := parseFile(filename, s)
	output, err := dinosql.Generate(result, dinosql.GenerateSettings{}, dinosql.PackageSettings{
		Name: "db",
	})
	if err != nil {
		t.Errorf("Failed to generate output: %v", err)
	}
	keep(output)
	// for k, v := range output {
	// 	fmt.Println(k)
	// 	fmt.Println(v)
	// 	fmt.Println("")
	// }
}

func TestParamType(t *testing.T) {
	s := NewSchema()
	result, _ := parseFile(filename, s)

	p := result.Queries[0].Params[0]
	keep(fmt.Sprintf("%v", p))
}

func keep(interface{}) {}

var mockSchema *Schema

func initMockSchema() {
	var schemaMap = make(map[string][]*sqlparser.ColumnDefinition)
	mockSchema = &Schema{
		tables: schemaMap,
	}
	schemaMap["users"] = []*sqlparser.ColumnDefinition{
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("first_name"),
			Type: sqlparser.ColumnType{
				Type:    "varchar",
				NotNull: true,
				// could add more here later if needed
			},
		},
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("last_name"),
			Type: sqlparser.ColumnType{
				Type:    "varchar",
				NotNull: false,
				// could add more here later if needed
			},
		},
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("id"),
			Type: sqlparser.ColumnType{
				Type:          "int",
				NotNull:       true,
				Autoincrement: true,
				// could add more here later if needed
			},
		},
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("age"),
			Type: sqlparser.ColumnType{
				Type:    "int",
				NotNull: true,
				// could add more here later if needed
			},
		},
	}
	schemaMap["orders"] = []*sqlparser.ColumnDefinition{
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("id"),
			Type: sqlparser.ColumnType{
				Type:          "int",
				NotNull:       true,
				Autoincrement: true,
				// could add more here later if needed
			},
		},
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("price"),
			Type: sqlparser.ColumnType{
				Type:          "DECIMAL(13, 4)",
				NotNull:       true,
				Autoincrement: true,
				// could add more here later if needed
			},
		},
		&sqlparser.ColumnDefinition{
			Name: sqlparser.NewColIdent("user_id"),
			Type: sqlparser.ColumnType{
				Type:    "int",
				NotNull: true,
				// could add more here later if needed
			},
		},
	}
}

func filterCols(allCols []*sqlparser.ColumnDefinition, tableNames map[string]struct{}) []*sqlparser.ColumnDefinition {
	filteredCols := []*sqlparser.ColumnDefinition{}
	for _, col := range allCols {
		if _, ok := tableNames[col.Name.String()]; ok {
			filteredCols = append(filteredCols, col)
		}
	}
	return filteredCols
}

func TestParseUnit(t *testing.T) {
	type expected struct {
		query  string
		schema *Schema
	}
	type testCase struct {
		input  expected
		output *Query
	}

	tests := []testCase{
		testCase{
			input: expected{
				query: `/* name: GetNameByID :one */
				SELECT first_name, last_name FROM users WHERE id = ?`,
				schema: mockSchema,
			},
			output: &Query{
				SQL:     `select first_name, last_name from users where id = :v1`,
				Columns: filterCols(mockSchema.tables["users"], map[string]struct{}{"first_name": struct{}{}, "last_name": struct{}{}}),
				Params: []*Param{&Param{
					originalName: ":v1",
					target: &sqlparser.ColName{
						Name:      sqlparser.NewColIdent("id"),
						Qualifier: sqlparser.TableName{},
					},
					typ: "int",
				}},
				Name:             "GetNameByID",
				Cmd:              ":one",
				defaultTableName: "users",
				schemaLookup:     mockSchema,
			},
		},
	}

	for _, testCase := range tests {
		q, err := parse(testCase.input.query, testCase.input.schema)
		if err != nil {
			t.Errorf("Parsing failed withe query: [%v]\n:schema: %v", query, spew.Sdump(testCase.input.schema))
		}
		if !reflect.DeepEqual(testCase.output, q) {
			t.Errorf("Parsing query returned differently than expected.")
			// t.Logf("Expected: %v\nResult: %v\n", spew.Sdump(testCase.output), spew.Sdump(q))
		}
	}
}

func TestParseLeadingComment(t *testing.T) {
	type expected struct {
		name string
		cmd  string
	}
	type testCase struct {
		input  string
		output expected
	}

	tests := []testCase{
		testCase{
			input:  "/* name: GetPeopleByID :many */",
			output: expected{name: "GetPeopleByID", cmd: ":many"},
		},
	}

	for _, tCase := range tests {
		qu := &Query{}
		err := qu.parseLeadingComment(tCase.input)
		if err != nil {
			t.Errorf("Failed to parse leading comment %v", err)
		}
		if qu.Name != tCase.output.name || qu.Cmd != tCase.output.cmd {
			t.Errorf("Leading comment parser returned unexpcted result: %v\n:\n Expected: [%v]\nRecieved:[%v]\n",
				err, spew.Sdump(tCase.output), spew.Sdump(qu))
		}

	}
}

func TestSchemaLookup(t *testing.T) {
	firstNameColDfn, err := mockSchema.schemaLookup("users", "first_name")
	if err != nil {
		t.Errorf("Failed to get column schema from mock schema: %v", err)
	}

	expected := filterCols(mockSchema.tables["users"], map[string]struct{}{"first_name": struct{}{}})
	if !reflect.DeepEqual(firstNameColDfn, expected[0]) {
		t.Errorf("Table schema lookup returned unexpected result")
	}
}
