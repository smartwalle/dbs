package dbs

import (
	"strconv"
)

type Placeholder interface {
	Replace(indx int) string
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

func (q *question) Replace(index int) string {
	return "?"
}

type dollar struct {
}

func (d *dollar) Replace(index int) string {
	return "$" + strconv.Itoa(index)
}
