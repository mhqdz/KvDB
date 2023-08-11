package kvDB_test

import (
	"KvDB/kvDB"
	"fmt"
	"testing"
)

type testData struct {
	S string
	I int
}

func TestDataMarshal(t *testing.T) {
	// 正常编码后的字符串 eyJTIjoiYWJjIiwiSSI6MTIzfQ== 长度是26
	b, n, err := kvDB.DataMarshal(testData{"abc", 123})
	if err != nil {
		t.Error(err)
	}

	if n != 28 {
		t.Errorf("编码后长度有误")
	}

	data := testData{}
	if err = kvDB.DataUnMarshal(&data, b); err != nil {
		t.Error(err)
	}
	fmt.Println("data:", data)
	if data.S != "abc" || data.I != 123 {
		t.Errorf("编码后数据有误")
	}
}
