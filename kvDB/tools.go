package kvDB

import "os"

// pathMkdir 创建文件的前置目录
func pathMkdir(filePath string) error {
	var productPrePath string
	for i := 0; i < len(filePath); i++ {
		if filePath[len(filePath)-i-1] == '/' || filePath[len(filePath)-i-1] == '\\' {
			productPrePath = filePath[:len(filePath)-i-1]
			break
		}
	}
	return os.MkdirAll(productPrePath, os.ModePerm)
}

// IsReservedWord 判断是否是不能作为key的保留字 目前只有mhKeyValueDatabaseLists
func IsReservedWord(key string) bool {
	return key == "mhKeyValueDatabaseLists"
}
