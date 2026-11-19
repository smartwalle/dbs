package dbs

type caseBranch struct {
	when any
	then any
	args []any
}

func (cb caseBranch) Clone() caseBranch {
	var ncb = caseBranch{
		when: cb.when,
		then: cb.then,
	}

	if raw, ok := cb.when.(SQLClause); ok {
		ncb.when = clone(raw)
	}

	if raw, ok := cb.then.(SQLClause); ok {
		ncb.then = clone(raw)
	}

	if len(cb.args) > 0 {
		ncb.args = make([]any, len(cb.args))
		for i, arg := range cb.args {
			if raw, ok := arg.(SQLClause); ok {
				ncb.args[i] = clone(raw)
			} else {
				ncb.args[i] = arg
			}
		}
	}

	return ncb
}

func (cb caseBranch) Write(w Writer) error {
	if _, err := w.WriteString(" WHEN "); err != nil {
		return err
	}

	args, err := writeCasePart(w, cb.when, cb.args)
	if err != nil {
		return err
	}

	if _, err = w.WriteString(" THEN "); err != nil {
		return err
	}

	args, err = writeCasePart(w, cb.then, args)
	if err != nil {
		return err
	}

	if len(args) > 0 {
		return ErrTooManyArguments
	}

	return nil
}

func writeCasePart(w Writer, value any, args []any) ([]any, error) {
	switch raw := value.(type) {
	case SQLClause:
		if err := raw.Write(w); err != nil {
			return nil, err
		}
	case string:
		return buildClause(w, raw, args)
	}
	return args, nil
}

type CaseClause struct {
	expression SQLClause
	branches   []caseBranch
	elseClause SQLClause
}

func Case(expression ...any) *CaseClause {
	var cc = &CaseClause{}
	if len(expression) == 0 {
		return cc
	}

	if raw, ok := expression[0].(SQLClause); ok {
		cc.expression = raw
	} else {
		cc.expression = SQL(expression[0], expression[1:]...)
	}
	return cc
}

func (cc *CaseClause) When(when, then any, args ...any) *CaseClause {
	var branch = caseBranch{
		when: when,
		then: then,
		args: args,
	}
	cc.branches = append(cc.branches, branch)
	return cc
}

func (cc *CaseClause) Else(sql any, args ...any) *CaseClause {
	if raw, ok := sql.(SQLClause); ok {
		cc.elseClause = raw
	} else {
		cc.elseClause = SQL(sql, args...)
	}
	return cc
}

func (cc *CaseClause) Clone() *CaseClause {
	if cc == nil {
		return nil
	}

	var ncc = &CaseClause{
		expression: clone(cc.expression),
		elseClause: clone(cc.elseClause),
	}
	if len(cc.branches) > 0 {
		ncc.branches = make([]caseBranch, len(cc.branches))
		for i, branch := range cc.branches {
			ncc.branches[i] = branch.Clone()
		}
	}
	return ncc
}

func (cc *CaseClause) Write(w Writer) error {
	if len(cc.branches) == 0 {
		return nil
	}

	if _, err := w.WriteString("CASE"); err != nil {
		return err
	}

	if cc.expression != nil {
		if err := w.WriteByte(' '); err != nil {
			return err
		}
		if err := cc.expression.Write(w); err != nil {
			return err
		}
	}

	for _, branch := range cc.branches {
		if err := branch.Write(w); err != nil {
			return err
		}
	}

	if cc.elseClause != nil {
		if _, err := w.WriteString(" ELSE "); err != nil {
			return err
		}
		if err := cc.elseClause.Write(w); err != nil {
			return err
		}
	}
	if _, err := w.WriteString(" END"); err != nil {
		return err
	}
	return nil
}

func (cc *CaseClause) SQL() (string, []any, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := cc.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}
