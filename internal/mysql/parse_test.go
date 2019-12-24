package mysql

import (
	"fmt"
	"testing"
)

const query = `
/* name: GetAllStudents :many */
SELECT school_id, id, school_id FROM students WHERE id = :id + ?
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
	fmt.Println(err)
}

func TestParse(t *testing.T) {
	// parseQuery(create)
	expectedQuery := Query{
		Name: "GetAllStudents",
		Cmd:  ":many",
	}
	schema := NewSchema()
	_, err := parse(create, schema)
	if err != nil {
		t.Error(err)
	}
	_, err = parse(query, schema)
	if err != nil {
		t.Error(err)
	}

	if expectedQuery.Name != "" {

	}
}

func TestParseLeadingComment(t *testing.T) {
	var query Query
	var testCases = []struct {
		input, expectedName, expectedCmd string
	}{{
		input:        "/* name: GetSchools :many */",
		expectedName: "GetSchools",
		expectedCmd:  ":many",
	},
	}
	for _, testCase := range testCases {
		err := query.parseLeadingComment(testCase.input)
		if query.Name != testCase.expectedName {
			t.Errorf("Leading comment parsing failed. %v", err)
		}
		if query.Cmd != testCase.expectedCmd {
			t.Errorf("Leading comment parsing failed. %v", err)
		}
	}
}

func TestColTypeLookup(t *testing.T) {

}
