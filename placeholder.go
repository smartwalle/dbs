package dbs

import "strconv"

type Placeholder interface {
	WriteTo(w Writer, indx int) error
}

var questionPlaceholder = &question{}

var dollarPlaceholder = &dollar{}

var globalPlaceholder Placeholder = questionPlaceholder

func UsePlaceholder(p Placeholder) {
	if p == nil {
		p = questionPlaceholder
	}
	globalPlaceholder = p
}

func GlobalPlaceholder() Placeholder {
	return globalPlaceholder
}

func QuestionPlaceholder() Placeholder {
	return questionPlaceholder
}

func DollarPlaceholder() Placeholder {
	return dollarPlaceholder
}

type question struct {
}

func (q *question) WriteTo(w Writer, _ int) (err error) {
	if err = w.WriteByte('?'); err != nil {
		return err
	}
	return nil
}

type dollar struct {
}

func (d *dollar) WriteTo(w Writer, idx int) (err error) {
	if err = w.WriteByte('$'); err != nil {
		return err
	}
	if _, err = w.WriteString(strconv.Itoa(idx)); err != nil {
		return err
	}
	return nil
}
