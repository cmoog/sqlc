package mysql

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/kyleconroy/sqlc/internal/dinosql"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Query holds the data for walking and validating mysql querys
type Query struct {
	SQL              string
	Columns          []*sqlparser.ColumnDefinition
	Params           []*Param
	Name             string
	Cmd              string // TODO: Pick a better name. One of: one, many, exec, execrows
	defaultTableName string // for columns that are not qualified
	schemaLookup     *Schema
}

func parseFile(filepath string, s *Schema) (*Result, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file [%v]: %v", filepath, err)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("Failed to read contents of file [%v]: %v", filepath, err)
	}
	rawQueries := strings.Split(string(contents), "\n\n")

	parsedQueries := []*Query{}

	for _, query := range rawQueries {
		result, err := parseQueryString(query, s)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse query in filepath [%v]: %v", filepath, err)
		}
		if result == nil {
			continue
		}
		parsedQueries = append(parsedQueries, result)
	}

	r := Result{
		Queries: parsedQueries,
		Schema:  s,
		Config:  dinosql.NewConfig(),
	}
	return &r, nil
}

func parseQueryString(query string, s *Schema) (*Query, error) {
	tree, err := sqlparser.Parse(query)

	if err != nil {
		return nil, err
	}

	switch tree := tree.(type) {
	case *sqlparser.Select:
		defaultTableName := getDefaultTable(tree)
		res, err := parseSelect(tree, query, s, defaultTableName)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse SELECT query: %v", err)
		}
		return res, nil
	case *sqlparser.Insert:
		insert, err := parseInsert(tree, query, s)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse INSERT query: %v", err)
		}
		return insert, nil
	case *sqlparser.Update:

	case *sqlparser.DDL:
		s.Add(tree)
		return nil, nil
	default:
		panic("Unsupported SQL statement type")
		// return &Query{}, nil
	}
	return nil, fmt.Errorf("Failed to parse query statement: %v", query)
}

func (q *Query) parseNameAndCmd() error {
	_, comments := sqlparser.SplitMarginComments(q.SQL)
	err := q.parseLeadingComment(comments.Leading)
	if err != nil {
		return fmt.Errorf("Failed to parse leading comment %v", err)
	}
	return nil
}

func parseSelect(tree *sqlparser.Select, query string, s *Schema, defaultTableName string) (*Query, error) {
	parsedQuery := Query{
		SQL:              query,
		defaultTableName: defaultTableName,
		schemaLookup:     s,
	}
	err := sqlparser.Walk(parsedQuery.visit, tree)
	if err != nil {
		return nil, err
	}

	var paramWalker ParamSearcher
	err = sqlparser.Walk(paramWalker.selectParamVisitor, tree)
	if err != nil {
		return nil, err
	}

	err = paramWalker.fillParamTypes(s, defaultTableName)
	if err != nil {
		return nil, err
	}
	parsedQuery.Params = paramWalker.params

	err = parsedQuery.parseNameAndCmd()
	if err != nil {
		return nil, err
	}

	return &parsedQuery, nil
}

func getDefaultTable(node *sqlparser.Select) string {
	var tableName string
	visit := func(node sqlparser.SQLNode) (bool, error) {
		switch v := node.(type) {
		case sqlparser.TableName:
			if name := v.Name.String(); name != "" {
				tableName = name
				return false, nil
			}
		}
		return true, nil
	}
	sqlparser.Walk(visit, node)
	return tableName
}

func parseInsert(node *sqlparser.Insert, query string, s *Schema) (*Query, error) {
	cols := node.Columns
	tableName := node.Table.Name.String()
	rows, ok := node.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("Unknown insert row type of %T", node.Rows)
	}

	params := []*Param{}

	for _, row := range rows {
		for colIx, item := range row {
			switch v := item.(type) {
			case *sqlparser.SQLVal:
				if v.Type == sqlparser.ValArg {
					colName := cols[colIx].String()
					colDfn, _ := s.schemaLookup(tableName, colName)
					p := &Param{
						originalName: string(v.Val),
						target:       cols[colIx],
						typ:          goTypeCol(colDfn),
					}
					params = append(params, p)
				}
			default:
				panic("Error occurred in parsing INSERT statement")
			}
		}
	}
	parsedQuery := &Query{
		SQL:              query,
		Params:           params,
		Columns:          nil,
		defaultTableName: tableName,
		schemaLookup:     s,
	}
	parsedQuery.parseNameAndCmd()
	return parsedQuery, nil
}

func (q *Query) parseLeadingComment(comment string) error {
	for _, line := range strings.Split(comment, "\n") {
		if !strings.HasPrefix(line, "/* name:") {
			continue
		}
		part := strings.Split(strings.TrimSpace(line), " ")
		if len(part) == 3 {
			return fmt.Errorf("missing query type [':one', ':many', ':exec', ':execrows']: %s", line)
		}
		if len(part) != 5 {
			return fmt.Errorf("invalid query comment: %s", line)
		}
		queryName := part[2]
		queryType := strings.TrimSpace(part[3])
		switch queryType {
		case ":one", ":many", ":exec", ":execrows":
		default:
			return fmt.Errorf("invalid query type: %s", queryType)
		}
		// if err := validateQueryName(queryName); err != nil {
		// 	return err
		// }
		q.Name = queryName
		q.Cmd = queryType
	}
	return nil
}

func (q *Query) visit(node sqlparser.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sqlparser.AliasedExpr:
		err := sqlparser.Walk(q.visitColNames, v)
		if err != nil {
			return false, err
		}
	default:
		// fmt.Printf("Did not handle %T\n", v)
	}
	return true, nil
}

func (q *Query) visitColNames(node sqlparser.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sqlparser.ColName:
		colTyp, err := q.schemaLookup.getColType(v, q.defaultTableName)
		if err != nil {
			return false, fmt.Errorf("Failed to get column type for [%v]: %v", v.Name.String(), err)
		}
		q.Columns = append(q.Columns, colTyp)
	}
	return true, nil
}

func GeneratePkg(filepath string, settings dinosql.GenerateSettings, pkg dinosql.PackageSettings) (map[string]string, error) {
	s := NewSchema()
	result, err := parseFile(filepath, s)
	if err != nil {
		return nil, err
	}
	output, err := dinosql.Generate(result, settings, pkg)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate output: %v", err)
	}

	return output, nil
}
