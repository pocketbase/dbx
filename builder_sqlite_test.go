// Copyright 2016 Qiang Xue. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqliteBuilder_QuoteSimpleTableName(t *testing.T) {
	b := getSqliteBuilder()
	assert.Equal(t, b.QuoteSimpleTableName(`abc`), "`abc`", "t1")
	assert.Equal(t, b.QuoteSimpleTableName("`abc`"), "`abc`", "t2")
	assert.Equal(t, b.QuoteSimpleTableName(`{{abc}}`), "`{{abc}}`", "t3")
	assert.Equal(t, b.QuoteSimpleTableName(`a.bc`), "`a.bc`", "t4")
}

func TestSqliteBuilder_QuoteSimpleColumnName(t *testing.T) {
	b := getSqliteBuilder()
	assert.Equal(t, b.QuoteSimpleColumnName(`abc`), "`abc`", "t1")
	assert.Equal(t, b.QuoteSimpleColumnName("`abc`"), "`abc`", "t2")
	assert.Equal(t, b.QuoteSimpleColumnName(`{{abc}}`), "`{{abc}}`", "t3")
	assert.Equal(t, b.QuoteSimpleColumnName(`a.bc`), "`a.bc`", "t4")
	assert.Equal(t, b.QuoteSimpleColumnName(`*`), `*`, "t5")
}

func TestSqliteBuilder_DropIndex(t *testing.T) {
	b := getSqliteBuilder()
	q := b.DropIndex("users", "idx")
	assert.Equal(t, q.SQL(), "DROP INDEX `idx`", "t1")
}

func TestSqliteBuilder_TruncateTable(t *testing.T) {
	b := getSqliteBuilder()
	q := b.TruncateTable("users")
	assert.Equal(t, q.SQL(), "DELETE FROM `users`", "t1")
}

func TestSqliteBuilder_RenameTable(t *testing.T) {
	b := getSqliteBuilder()
	q := b.RenameTable("usersOld", "usersNew")
	assert.Equal(t, q.SQL(), "ALTER TABLE `usersOld` RENAME TO `usersNew`", "t1")
}

func TestSqliteBuilder_AlterColumn(t *testing.T) {
	b := getSqliteBuilder()
	q := b.AlterColumn("users", "name", "int")
	assert.NotEqual(t, q.LastError, nil, "t1")
}

func TestSqliteBuilder_AddPrimaryKey(t *testing.T) {
	b := getSqliteBuilder()
	q := b.AddPrimaryKey("users", "pk", "id1", "id2")
	assert.NotEqual(t, q.LastError, nil, "t1")
}

func TestSqliteBuilder_DropPrimaryKey(t *testing.T) {
	b := getSqliteBuilder()
	q := b.DropPrimaryKey("users", "pk")
	assert.NotEqual(t, q.LastError, nil, "t1")
}

func TestSqliteBuilder_AddForeignKey(t *testing.T) {
	b := getSqliteBuilder()
	q := b.AddForeignKey("users", "fk", []string{"p1", "p2"}, []string{"f1", "f2"}, "profile", "opt")
	assert.NotEqual(t, q.LastError, nil, "t1")
}

func TestSqliteBuilder_DropForeignKey(t *testing.T) {
	b := getSqliteBuilder()
	q := b.DropForeignKey("users", "fk")
	assert.NotEqual(t, q.LastError, nil, "t1")
}

func TestSqliteBuilder_BuildUnion(t *testing.T) {
	b := getSqliteBuilder()
	qb := b.QueryBuilder()

	params := Params{}
	ui := UnionInfo{false, b.NewQuery("SELECT names").Bind(Params{"id": 1})}
	sql := qb.BuildUnion([]UnionInfo{ui}, params)
	expected := "UNION SELECT names"
	assert.Equal(t, expected, sql, "BuildUnion@1")
	assert.Equal(t, 1, len(params), "len(params)@1")

	params = Params{}
	ui = UnionInfo{true, b.NewQuery("SELECT names")}
	sql = qb.BuildUnion([]UnionInfo{ui}, params)
	expected = "UNION ALL SELECT names"
	assert.Equal(t, expected, sql, "BuildUnion@2")
	assert.Equal(t, 0, len(params), "len(params)@2")

	sql = qb.BuildUnion([]UnionInfo{}, nil)
	expected = ""
	assert.Equal(t, expected, sql, "BuildUnion@3")

	ui = UnionInfo{true, b.NewQuery("SELECT names")}
	ui2 := UnionInfo{false, b.NewQuery("SELECT ages")}
	sql = qb.BuildUnion([]UnionInfo{ui, ui2}, nil)
	expected = "UNION ALL SELECT names UNION SELECT ages"
	assert.Equal(t, expected, sql, "BuildUnion@4")
}

func TestSqliteBuilder_CombineUnion(t *testing.T) {
	b := getSqliteBuilder()
	qb := b.QueryBuilder()

	sql := qb.CombineUnion("p1", "p2")
	assert.Equal(t, "p1 p2", sql)
}

func getSqliteBuilder() Builder {
	db := getDB()
	b := NewSqliteBuilder(db, db.sqlDB)
	db.Builder = b
	return b
}
