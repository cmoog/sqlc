package mysql

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type Param struct {
	originalName string
	target       sqlparser.SQLNode
	typ          string
}

// ParamSearcher finds the type of query params "?"
type ParamSearcher struct {
	parent sqlparser.SQLNode
	params []*Param
}

func (p *ParamSearcher) selectParamVisitor(node sqlparser.SQLNode) (bool, error) {
	switch v := node.(type) {
	case *sqlparser.SQLVal:
		if v.Type != sqlparser.ValArg {
			break
		}

		// if last param
		if l := len(p.params); l > 0 && p.params[l-1].target == nil {
			p.params[l-1].target = p.parent
		}

		newParam := Param{
			originalName: string(v.Val),
		}
		if p.parent != nil {
			newParam.target = p.parent
		}

		p.params = append(p.params, &newParam)

	case *sqlparser.ColName, *sqlparser.Limit:
		p.parent = v

	default:
		// fmt.Printf("Did not handle %T\n", v)
	}

	return true, nil
}

func (p *ParamSearcher) fillParamTypes(s *Schema, defaultTableName string) error {
	for _, param := range p.params {
		switch target := param.target.(type) {
		case *sqlparser.ColName:
			colDfn, err := s.schemaLookup(defaultTableName, target.Name.String())
			if err != nil {
				return err
			}
			param.typ = goTypeCol(colDfn)

		case *sqlparser.Limit:
			param.typ = "uint32"
		}
	}
	return nil
}

// Name gives the name string for use as a Go identifier
func (p Param) Name() string {
	original := string(p.originalName)

	cleanParamName := func(str string) string {
		if !strings.HasPrefix(original, ":v") {
			return original[1:]
		}
		if str != "" {
			return str
		}
		num := string(original[2])
		return fmt.Sprintf("param%v", num)
	}

	switch v := p.target.(type) {
	case *sqlparser.ColName:
		return cleanParamName(v.Name.String())
	case sqlparser.ColIdent:
		return cleanParamName(v.String())
	case *sqlparser.Limit:
		return "limit"
	}
	num := string(original[2])
	return fmt.Sprintf("param%v", num)
}
