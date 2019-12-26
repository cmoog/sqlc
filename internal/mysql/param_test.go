package mysql

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestParamSearcher(t *testing.T) {
	type testCase struct {
		input         string
		output        []*Param
		expectedNames []string
	}

	tests := []testCase{
		testCase{
			input: "SELECT first_name, id, last_name FROM users WHERE id < ?",
			output: []*Param{&Param{
				originalName: ":v1",
				target: &sqlparser.ColName{
					Name:      sqlparser.NewColIdent("id"),
					Qualifier: sqlparser.TableName{},
				},
				typ: "int",
			},
			},
			expectedNames: []string{"id"},
		},
		testCase{
			input: `SELECT
								users.id,
								users.first_name,
								orders.price
							FROM
								orders
							LEFT JOIN users ON orders.user_id = users.id
							WHERE orders.price > :minPrice`,
			output: []*Param{&Param{
				originalName: ":minPrice",
				target: &sqlparser.ColName{
					Name: sqlparser.NewColIdent("price"),
					Qualifier: sqlparser.TableName{
						Name: sqlparser.NewTableIdent("orders"),
					},
				},
				typ: "float64",
			},
			},
			expectedNames: []string{"minPrice"},
		},
		testCase{
			input: "SELECT first_name, id, last_name FROM users WHERE id = :targetID",
			output: []*Param{&Param{
				originalName: ":targetID",
				target: &sqlparser.ColName{
					Name:      sqlparser.NewColIdent("id"),
					Qualifier: sqlparser.TableName{},
				},
				typ: "int",
			},
			},
			expectedNames: []string{"targetID"},
		},
		testCase{
			input: "SELECT first_name, last_name FROM users WHERE age < :maxAge AND last_name = :inFamily",
			output: []*Param{
				&Param{
					originalName: ":maxAge",
					target: &sqlparser.ColName{
						Name:      sqlparser.NewColIdent("age"),
						Qualifier: sqlparser.TableName{},
					},
					typ: "int",
				},
				&Param{
					originalName: ":inFamily",
					target: &sqlparser.ColName{
						Name:      sqlparser.NewColIdent("last_name"),
						Qualifier: sqlparser.TableName{},
					},
					typ: "sql.NullString",
				},
			},
			expectedNames: []string{"maxAge", "inFamily"},
		},
		testCase{
			input: "SELECT first_name, last_name FROM users LIMIT ?",
			output: []*Param{
				&Param{
					originalName: ":v1",
					target: &sqlparser.Limit{
						Offset:   nil,
						Rowcount: sqlparser.NewValArg([]byte(":v1")),
					},
					typ: "uint32",
				},
			},
			expectedNames: []string{"limit"},
		},
	}
	for _, tCase := range tests {
		var searcher ParamSearcher
		tree, err := sqlparser.Parse(tCase.input)
		if err != nil {
			t.Errorf("Failed to parse input query")
		}
		sqlparser.Walk(searcher.paramVisitor, tree)

		// TODO: get this out of the unit test and/or deprecate defaultTable
		defaultTable := getDefaultTable(tree)
		err = searcher.fillParamTypes(mockSchema, defaultTable)

		if !reflect.DeepEqual(searcher.params, tCase.output) {
			t.Errorf("Param searcher returned unexpected result\nResult: %v\nExpected: %v",
				spew.Sdump(searcher.params), spew.Sdump(tCase.output))
		}
		if len(searcher.params) != len(tCase.expectedNames) {
			t.Errorf("Insufficient test cases. Mismatch in length of expected param names and parsed params")
		}
		for ix, p := range searcher.params {
			if p.Name() != tCase.expectedNames[ix] {
				t.Errorf("Derived param does not match expected output.\nResult: %v\nExpected: %v",
					p.Name(), tCase.expectedNames[ix])
			}
		}
	}
}
