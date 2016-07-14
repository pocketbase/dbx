// Copyright 2016 Qiang Xue. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dbx

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFieldMapFunc(t *testing.T) {
	tests := []struct {
		input, output string
	}{
		{"Name", "name"},
		{"FirstName", "first_name"},
		{"Name0", "name0"},
		{"ID", "id"},
		{"UserID", "user_id"},
		{"User0ID", "user0_id"},
		{"MyURL", "my_url"},
		{"URLPath", "urlpath"},
		{"MyURLPath", "my_urlpath"},
		{"First_Name", "first_name"},
		{"first_name", "first_name"},
		{"_FirstName", "_first_name"},
		{"_First_Name", "_first_name"},
	}
	for _, test := range tests {
		r := DefaultFieldMapFunc(test.input)
		assert.Equal(t, test.output, r, test.input)
	}
}

func Test_concat(t *testing.T) {
	assert.Equal(t, "a.b", concat("a", "b"))
	assert.Equal(t, "a", concat("a", ""))
	assert.Equal(t, "b", concat("", "b"))
}

func Test_parseTag(t *testing.T) {
	name, pk := parseTag("abc")
	assert.Equal(t, "abc", name)
	assert.False(t, pk)

	name, pk = parseTag("pk,abc")
	assert.Equal(t, "abc", name)
	assert.True(t, pk)

	name, pk = parseTag("pk")
	assert.Equal(t, "", name)
	assert.True(t, pk)
}

func Test_indirect(t *testing.T) {
	var a int
	assert.Equal(t, reflect.ValueOf(a).Kind(), indirect(reflect.ValueOf(a)).Kind())
	var b *int
	bi := indirect(reflect.ValueOf(&b))
	assert.Equal(t, reflect.ValueOf(a).Kind(), bi.Kind())
	if assert.NotNil(t, b) {
		assert.Equal(t, 0, *b)
	}
}

func Test_structValue_columns(t *testing.T) {
	customer := Customer{
		ID:     1,
		Name:   "abc",
		Status: 2,
		Email:  "abc@example.com",
	}
	sv := newStructValue(&customer, DefaultFieldMapFunc)
	cols := sv.columns(nil, nil)
	assert.Equal(t, map[string]interface{}{"id": 1, "name": "abc", "status": 2, "email": "abc@example.com", "address": sql.NullString{}}, cols)

	cols = sv.columns([]string{"ID", "name"}, nil)
	assert.Equal(t, map[string]interface{}{"id": 1}, cols)

	cols = sv.columns([]string{"ID", "Name"}, []string{"ID"})
	assert.Equal(t, map[string]interface{}{"name": "abc"}, cols)

	cols = sv.columns(nil, []string{"ID", "Address"})
	assert.Equal(t, map[string]interface{}{"name": "abc", "status": 2, "email": "abc@example.com"}, cols)

	sv = newStructValue(&customer, nil)
	cols = sv.columns([]string{"ID", "Name"}, []string{"ID"})
	assert.Equal(t, map[string]interface{}{"Name": "abc"}, cols)
}

type FA struct {
	A1 string
	A2 int
}

type FB struct {
	B1 string
}

//
//func TestGetFieldMap(t *testing.T) {
//	var a struct {
//		X1 string
//		FA
//		X2 int
//		B  *FB
//		FB `db:"c"`
//		c  int
//	}
//	ta := reflect.TypeOf(a)
//	r := getFieldMap(ta, DefaultFieldMapFunc)
//
//	v, _ := json.Marshal(r)
//	assert.Equal(t, `{"a1":[1,0],"a2":[1,1],"b.b1":[3,0],"c.b1":[4,0],"x1":[0],"x2":[2]}`, string(v))
//}