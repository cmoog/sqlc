package mysql

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSelectParamSearcher(t *testing.T) {
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
				typ:          "int",
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
				typ:          "float64",
			},
			},
			expectedNames: []string{"minPrice"},
		},
		testCase{
			input: "SELECT first_name, id, last_name FROM users WHERE id = :targetID",
			output: []*Param{&Param{
				originalName: ":targetID",
				typ:          "int",
			},
			},
			expectedNames: []string{"targetID"},
		},
		testCase{
			input: "SELECT first_name, last_name FROM users WHERE age < :maxAge AND last_name = :inFamily",
			output: []*Param{
				&Param{
					originalName: ":maxAge",
					typ:          "int",
				},
				&Param{
					originalName: ":inFamily",
					typ:          "sql.NullString",
				},
			},
			expectedNames: []string{"maxAge", "inFamily"},
		},
		testCase{
			input: "SELECT first_name, last_name FROM users LIMIT ?",
			output: []*Param{
				&Param{
					originalName: ":v1",
					typ:          "uint32",
				},
			},
			expectedNames: []string{"limit"},
		},
	}
	for _, tCase := range tests {
		tree, err := sqlparser.Parse(tCase.input)
		if err != nil {
			t.Errorf("Failed to parse input query")
		}
		selectStm, ok := tree.(*sqlparser.Select)
		if !ok {
			t.Errorf("Test case is not SELECT statement as expected")
		}

		// TODO: get this out of the unit test and/or deprecate defaultTable
		defaultTable := getDefaultTable(selectStm)
		keep(defaultTable)

		// if !reflect.DeepEqual(searcher.params, tCase.output) {
		// 	t.Errorf("Param searcher returned unexpected result\nResult: %v\nExpected: %v",
		// 		spew.Sdump(searcher.params), spew.Sdump(tCase.output))
		// }
		// if len(searcher.params) != len(tCase.expectedNames) {
		// 	t.Errorf("Insufficient test cases. Mismatch in length of expected param names and parsed params")
		// }
		// for ix, p := range searcher.params {
		// 	if p.Name() != tCase.expectedNames[ix] {
		// 		t.Errorf("Derived param does not match expected output.\nResult: %v\nExpected: %v",
		// 			p.Name(), tCase.expectedNames[ix])
		// 	}
		// }
	}
}

func TestInsertParamSearcher(t *testing.T) {
	type testCase struct {
		input         string
		output        []*Param
		expectedNames []string
	}

	tests := []testCase{
		testCase{
			input: "INSERT INTO users (first_name, last_name) VALUES (?, ?)",
			output: []*Param{
				&Param{
					originalName: ":v1",
					name:         "first_name",
					typ:          "string",
				},
				&Param{
					originalName: ":v2",
					name:         "last_name",
					typ:          "sql.NullString",
				},
			},
			expectedNames: []string{"first_name", "last_name"},
		},
	}
	for _, tCase := range tests {
		tree, err := sqlparser.Parse(tCase.input)
		if err != nil {
			t.Errorf("Failed to parse input query")
		}
		insertStm, ok := tree.(*sqlparser.Insert)
		if !ok {
			t.Errorf("Test case is not SELECT statement as expected")
		}
		result, err := parseInsert(insertStm, tCase.input, mockSchema, mockSettings)
		if err != nil {
			t.Errorf("Failed to parse insert statement.")
		}

		if !reflect.DeepEqual(result.Params, tCase.output) {
			t.Errorf("Param searcher returned unexpected result\nResult: %v\nExpected: %v\nQuery: %s",
				spew.Sdump(result.Params), spew.Sdump(tCase.output), tCase.input)
		}
		if len(result.Params) != len(tCase.expectedNames) {
			t.Errorf("Insufficient test cases. Mismatch in length of expected param names and parsed params")
		}
		for ix, p := range result.Params {
			if p.name != tCase.expectedNames[ix] {
				t.Errorf("Derived param does not match expected output.\nResult: %v\nExpected: %v",
					p.name, tCase.expectedNames[ix])
			}
		}
	}
}
