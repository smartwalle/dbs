package dbs

import "strconv"

type Placeholder interface {
	Replace(indx int) string
}

var QuestionPlaceholder = &question{}

var DollarPlaceholder = &dollar{}

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
