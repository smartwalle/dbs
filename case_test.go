package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestCaseClause_SQL(t *testing.T) {
	tests := []struct {
		name       string
		clause     dbs.SQLClause
		expectSQL  string
		expectArgs []any
	}{
		{
			name: "Searched CASE",
			clause: dbs.Case().
				When(dbs.SQL("status = ?", 1), dbs.SQL("?", "正常")).
				When(dbs.SQL("status = ?", 2), dbs.SQL("?", "禁用")).
				Else(dbs.SQL("?", "未知")),
			expectSQL:  "CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END",
			expectArgs: ExpectArgs(1, "正常", 2, "禁用", "未知"),
		},
		{
			name: "Simple CASE",
			clause: dbs.Case(dbs.SQL("status")).
				When("?", "?", 1, "正常").
				When("?", "?", 2, "禁用").
				Else("?", "未知"),
			expectSQL:  "CASE status WHEN ? THEN ? WHEN ? THEN ? ELSE ? END",
			expectArgs: ExpectArgs(1, "正常", 2, "禁用", "未知"),
		},
		{
			name: "Expression arguments",
			clause: dbs.Case(dbs.SQL("COALESCE(status, ?)", 0)).
				When(dbs.SQL("?", 1), dbs.SQL("?", "正常")),
			expectSQL:  "CASE COALESCE(status, ?) WHEN ? THEN ? END",
			expectArgs: ExpectArgs(0, 1, "正常"),
		},
		{
			name: "Multiple expressions",
			clause: dbs.Case(
				dbs.SQL("COALESCE(status, ?)", 0),
				dbs.SQL("ignored"),
			).When(dbs.SQL("?", 1), dbs.SQL("?", "正常")),
			expectSQL:  "CASE COALESCE(status, ?) WHEN ? THEN ? END",
			expectArgs: ExpectArgs(0, 1, "正常"),
		},
		{
			name: "Shared arguments",
			clause: dbs.Case().
				When("status = ?", "?", 1, "正常").
				When("status = ?", "?", 2, "禁用").
				Else(dbs.SQL("?", "未知")),
			expectSQL:  "CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END",
			expectArgs: ExpectArgs(1, "正常", 2, "禁用", "未知"),
		},
		{
			name: "Mixed SQLClause and string",
			clause: dbs.Case().
				When(dbs.SQL("status = ?", 1), "?", "正常").
				When("status = ?", dbs.SQL("?", "禁用"), 2),
			expectSQL:  "CASE WHEN status = ? THEN ? WHEN status = ? THEN ? END",
			expectArgs: ExpectArgs(1, "正常", 2, "禁用"),
		},
		{
			name: "Without ELSE",
			clause: dbs.Case().
				When(dbs.SQL("status = ?", 1), dbs.SQL("?", "正常")),
			expectSQL:  "CASE WHEN status = ? THEN ? END",
			expectArgs: ExpectArgs(1, "正常"),
		},
		{
			name: "ELSE before WHEN and repeated ELSE",
			clause: dbs.Case().
				Else(dbs.SQL("?", "旧值")).
				When(dbs.SQL("status = ?", 1), dbs.SQL("?", "正常")).
				Else(dbs.SQL("?", "新值")),
			expectSQL:  "CASE WHEN status = ? THEN ? ELSE ? END",
			expectArgs: ExpectArgs(1, "正常", "新值"),
		},
		{
			name:       "Without WHEN",
			clause:     dbs.Case().Else(dbs.SQL("?", "未知")),
			expectSQL:  "",
			expectArgs: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checkClause(t, test.clause, test.expectSQL, test.expectArgs)
		})
	}
}

func TestCaseClause_Nested(t *testing.T) {
	t.Run("Nested CASE in THEN", func(t *testing.T) {
		clause := dbs.Case().
			When(
				dbs.SQL("status = ?", 1),
				dbs.Case().
					When(dbs.SQL("score >= ?", 60), dbs.SQL("?", "通过")).
					Else(dbs.SQL("?", "未通过")),
			).
			Else(dbs.SQL("?", "未启用"))

		checkClause(
			t,
			clause,
			"CASE WHEN status = ? THEN CASE WHEN score >= ? THEN ? ELSE ? END ELSE ? END",
			ExpectArgs(1, 60, "通过", "未通过", "未启用"),
		)
	})

	t.Run("Nested CASE in WHEN", func(t *testing.T) {
		clause := dbs.Case().
			When(
				dbs.SQL("(?) = ?", dbs.Case().
					When(dbs.SQL("type = ?", 1), dbs.SQL("?", 10)).
					Else(dbs.SQL("?", 20)), 10),
				dbs.SQL("?", "匹配"),
			).
			Else(dbs.SQL("?", "不匹配"))

		checkClause(
			t,
			clause,
			"CASE WHEN (CASE WHEN type = ? THEN ? ELSE ? END) = ? THEN ? ELSE ? END",
			ExpectArgs(1, 10, 20, 10, "匹配", "不匹配"),
		)
	})

	t.Run("Nested CASE in expression", func(t *testing.T) {
		clause := dbs.Case(
			dbs.Case().
				When(dbs.SQL("type = ?", 1), dbs.SQL("?", 10)).
				Else(dbs.SQL("?", 20)),
		).
			When(dbs.SQL("?", 10), dbs.SQL("?", "类型一")).
			Else(dbs.SQL("?", "其他"))

		checkClause(
			t,
			clause,
			"CASE CASE WHEN type = ? THEN ? ELSE ? END WHEN ? THEN ? ELSE ? END",
			ExpectArgs(1, 10, 20, 10, "类型一", "其他"),
		)
	})
}

func TestCaseClause_Clone(t *testing.T) {
	inner := dbs.Case().
		When(dbs.SQL("score >= ?", 60), dbs.SQL("?", "通过")).
		Else(dbs.SQL("?", "未通过"))

	original := dbs.Case().
		When(dbs.SQL("status = ?", 1), inner).
		Else(dbs.SQL("?", "未启用"))

	cloned := original.Clone()

	inner.When(dbs.SQL("score >= ?", 90), dbs.SQL("?", "优秀"))
	original.When(dbs.SQL("status = ?", 2), dbs.SQL("?", "禁用"))

	checkClause(
		t,
		cloned,
		"CASE WHEN status = ? THEN CASE WHEN score >= ? THEN ? ELSE ? END ELSE ? END",
		ExpectArgs(1, 60, "通过", "未通过", "未启用"),
	)

	checkClause(
		t,
		original,
		"CASE WHEN status = ? THEN CASE WHEN score >= ? THEN ? WHEN score >= ? THEN ? ELSE ? END WHEN status = ? THEN ? ELSE ? END",
		ExpectArgs(1, 60, "通过", 90, "优秀", "未通过", 2, "禁用", "未启用"),
	)
}

func TestCaseClause_Builder(t *testing.T) {
	clause := dbs.Case().
		When(dbs.SQL("status = ?", 1), dbs.SQL("?", "正常")).
		When(dbs.SQL("status = ?", 2), dbs.SQL("?", "禁用")).
		Else(dbs.SQL("?", "未知"))

	t.Run("SelectBuilder", func(t *testing.T) {
		builder := dbs.NewSelectBuilder().
			Table("user").
			Select("id").
			Select(clause).
			Where("id = ?", 100)

		checkClause(
			t,
			builder,
			"SELECT id,CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END FROM user WHERE id = ?",
			ExpectArgs(1, "正常", 2, "禁用", "未知", 100),
		)
	})

	t.Run("UpdateBuilder", func(t *testing.T) {
		builder := dbs.NewUpdateBuilder().
			Table("user").
			Set("status_name", clause).
			Where("id = ?", 100)

		checkClause(
			t,
			builder,
			"UPDATE user SET status_name=CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ? END WHERE id = ?",
			ExpectArgs(1, "正常", 2, "禁用", "未知", 100),
		)
	})
}
