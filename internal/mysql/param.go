package mysql

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type Param struct {
	originalName string
	colName      *sqlparser.ColName
	colDfn       *sqlparser.ColumnDefinition
}

// ParamSearcher finds the type of query params "?"
type ParamSearcher struct {
	parent *sqlparser.ColName
	params []*Param
}

func (p *ParamSearcher) paramVisitor(node sqlparser.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sqlparser.SQLVal:
		if v.Type != sqlparser.ValArg {
			break
		}
		if l := len(p.params); l > 0 && p.params[l-1].colName == nil {
			p.params[l-1].colName = p.parent
		}

		newParam := Param{originalName: string(v.Val)}
		if p.parent != nil {
			newParam.colName = p.parent
		}

		p.params = append(p.params, &newParam)

	case *sqlparser.ColName:
		p.parent = v

	default:
		// fmt.Printf("Did not handle %T\n", v)
	}

	return true, nil
}

func (p *ParamSearcher) fillWithColDefinitions(s *Schema, defaultTableName string) error {
	for _, param := range p.params {
		colDfn, err := s.schemaLookup(defaultTableName, param.colName.Name.String())
		if err != nil {
			return err
		}
		param.colDfn = colDfn
	}
	return nil
}

// Name gives the name string for use as a Go identifier
func (p Param) Name() string {
	str := string(p.originalName)
	if p.colName != nil && !p.colName.Name.IsEmpty() {
		return p.colName.Name.String()
	}
	if strings.HasPrefix(str, ":v") && len(str) > 2 {
		num := string(str[2])
		return fmt.Sprintf("param%v", num)
	}
	return fmt.Sprintf("dollar_%s", str[1:])
}

func (p Param) GoType() string {
	colDfn := p.colDfn
	if colDfn == nil {
		return ""
	}
	return goTypeCol(&colDfn.Type)
}
