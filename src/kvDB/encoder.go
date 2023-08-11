package kvDB

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

// encodeData 数据加密 这里的加密方式是转base64然后去除后面的"=="
func encodeData(dst []byte) []byte {
	bData := []byte(base64.StdEncoding.EncodeToString(dst))
	return bData
}

// decodeData 数据解密
func decodeData(dst []byte) (data []byte, err error) {
	data, err = base64.StdEncoding.DecodeString(string(dst))
	// fmt.Printf("string(data): %v\n", string(data))
	return
}

// DataMarshal 数据加密转换成存储的字符串,返回值:加密后的字符串,字符串长度,错误
func DataMarshal(data any) ([]byte, int, error) {
	if bData, err := json.Marshal(data); err == nil {
		sData := encodeData(bData)
		return sData, len(sData), nil
	} else {
		return nil, 0, err
	}
}

// DataUnMarshal 把从文件中读取的数据src 解码到dst中
// dst作为数据接收者 必须是一个指针
func DataUnMarshal(dst any, src []byte) error {
	if reflect.ValueOf(dst).Type().Kind() != reflect.Ptr {
		return fmt.Errorf("接收数据的dst必须是一个指针")
	}
	data, err := decodeData(src)
	if err != nil {
		return fmt.Errorf("源数据解码错误" + err.Error())
	}
	err = json.Unmarshal(data, dst)

	if err != nil && err.Error() == "json: cannot unmarshal object into Go value of type string" {
		return &masterErr{
			msg: "源数据为无法转换为string的数据,请使用getjson [key]查看",
			Err: nil,
		}
	}
	return err
}

// DataUnMarshalToJson 把从文件中读取的数据src 解码成一个json的byte数组
func DataUnMarshalToJson(src []byte) ([]byte, error) {
	data, err := decodeData(src)
	return data, err
}
