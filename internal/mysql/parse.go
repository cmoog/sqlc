package mysql

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/kyleconroy/sqlc/internal/dinosql"
	sql "vitess.io/vitess/go/vt/sqlparser"
)

// Query holds the data for walking and validating mysql querys
type Query struct {
	SQL              string
	Columns          []*sql.ColumnDefinition
	Params           []*sql.SQLVal
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
		result, err := parse(query, s)
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

func parse(query string, s *Schema) (*Query, error) {
	tree, err := sql.Parse(query)

	if err != nil {
		return nil, err
	}

	switch tree := tree.(type) {
	case *sql.Select:
		defaultTableName := getDefaultTable(tree)
		res, err := parseQuery(tree, query, s, defaultTableName)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse query: %v", err)
		}
		return res, nil
	case *sql.Insert, *sql.Update:
	case *sql.DDL:
		s.Add(tree)
		return nil, nil
	}
	return nil, fmt.Errorf("Failed to parse query statement: ")
}

func getDefaultTable(node sql.SQLNode) string {
	var tableName string
	visit := func(node sql.SQLNode) (bool, error) {
		switch v := node.(type) {
		case sql.TableName:
			if name := v.Name.String(); name != "" {
				tableName = name
				return false, nil
			}
		}
		return true, nil
	}
	sql.Walk(visit, node)
	return tableName
}

func parseQuery(tree sql.Statement, query string, s *Schema, defaultTableName string) (*Query, error) {
	parsedQuery := Query{
		SQL:              query,
		defaultTableName: defaultTableName,
		schemaLookup:     s,
	}
	err := sql.Walk(parsedQuery.visit, tree)

	if err != nil {
		return nil, err
	}

	_, comments := sql.SplitMarginComments(query)
	err = parsedQuery.parseLeadingComment(comments.Leading)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse leading comment %v", err)
	}
	return &parsedQuery, nil
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

func (q *Query) visit(node sql.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sql.SQLVal:
		q.Params = append(q.Params, v)
	case *sql.AliasedExpr:
		err := sql.Walk(q.visitSelect, v)
		if err != nil {
			return false, err
		}
	default:
		// fmt.Printf("Did not handle %T\n", v)
	}
	return true, nil
}

func (q *Query) visitSelect(node sql.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sql.ColName:
		colTyp, err := q.schemaLookup.getColType(v, q.defaultTableName)
		if err != nil {
			return false, fmt.Errorf("Failed to get column type for [%v]: %v", v.Name.String(), err)
		}
		q.Columns = append(q.Columns, colTyp)
	}
	return true, nil
}

// NewSchema gives a newly instantiated MySQL schema map
func NewSchema() *Schema {
	return &Schema{
		tables: make(map[string]([]*sql.ColumnDefinition)),
	}
}

// Schema proves that information for mapping columns in queries to their respective table definitions
// and validating that they are correct so as to map to the correct Go type
type Schema struct {
	tables map[string]([]*sql.ColumnDefinition)
}

func (s *Schema) getColType(node sql.SQLNode, defaultTableName string) (*sql.ColumnDefinition, error) {
	col, ok := node.(*sql.ColName)
	if !ok {
		return nil, fmt.Errorf("Attempted to determine the type of a non-column node")
	}
	// colName := col.Name.String()
	if !col.Qualifier.IsEmpty() {
		return s.schemaLookup(col.Qualifier.Name.String(), col.Name.String())
	}
	return s.schemaLookup(defaultTableName, col.Name.String())
}

// Add add a MySQL table definition to the schema map
func (s *Schema) Add(table *sql.DDL) {
	name := table.Table.Name.String()
	s.tables[name] = table.TableSpec.Columns
}

func (s *Schema) schemaLookup(table string, col string) (*sql.ColumnDefinition, error) {
	cols, ok := s.tables[table]
	if !ok {
		return nil, fmt.Errorf("Table [%v] not found in Schema", table)
	}

	for _, colDef := range cols {
		if colDef.Name.EqualString(col) {
			return colDef, nil
		}
	}

	return nil, fmt.Errorf("Column [%v] not found in table [%v]", col, table)
}
