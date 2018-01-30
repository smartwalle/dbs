package dbs

import (
	"fmt"
	"testing"
)

func TestSelectBuilder_Select(t *testing.T) {
	//var sb = NewSelectBuilder()
	//sb.Selects("u.id", "u.name")
	//sb.Select("u.email")
	//sb.From("user", "AS u")
	//sb.LeftJoin("student", "AS s ON s.user_id=u.id AND s.type=?", 1001)
	//sb.RightJoin("course", "AS c ON c.user_id=u.id AND c.type=?", 1002)
	//
	//var ws = AND()
	//ws.Append(OR(SQL("u.id=?", 10), SQL("u.email=?", "qq@qq.com")))
	//ws.Append(OR(SQL("u.id=?", 11), SQL("u.email=?", "qq@qq.com")))
	//sb.Where(ws)
	//
	//fmt.Println(sb.ToSQL())
}

func TestSelectBuilder_Select2(t *testing.T) {
	//var sb1 = NewSelectBuilder()
	//sb1.Select("u.id")
	//sb1.From("user", "AS u")
	//sb1.Where(SQL("u.id=?", 10).Append("OR u.id=?", 20))
	//fmt.Println(sb1.ToSQL())
	//
	//var sb2 = NewSelectBuilder()
	//sb2.Select("s.id")
	//sb2.From("stu", "AS s")
	//sb2.Where(NewClause("s.user_id=", sb1))
	//fmt.Println(sb2.ToSQL())
}

func TestSelectBuilder_Select3(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select("u.name")
	sb.From("user", "AS u")

	sb.Where("")

	//sb.Where("a=? AND cc =?", 10, 200)
	//sb.Where("d=?", "110")
	//sb.Where(SQL("b=?", 20))
	//sb.Where(OR(SQL("c=?", 30), SQL("d=?", 40)))
	fmt.Println(sb.ToSQL())
}