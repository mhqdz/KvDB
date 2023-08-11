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
