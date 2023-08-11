package main

import (
	"KvDB/kvDB"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	dbConf := kvDB.InitConf()
	db, _ := kvDB.InitDBMaster(dbConf)
	defer db.Close()
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("欢迎使用mhKvDB:")

	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		text = strings.ToLower(text)

		if text == "help" {
			fmt.Println(" - set: set [key] [value] ,把key=value存入数据库")
			fmt.Println(" - get: get [key] ,获取key对应的值")
			fmt.Println(" - getjson: getjson [key] ,获取key对应的json,对于不能解析成string的go结构体,请使用此命令")
			fmt.Println(" - getkeys: 打印所有的key")
			fmt.Println(" - getall: getall ,获取key 以及对应值的json格式数据")
			fmt.Println(" - del: del [key] ,获取key对应的值")
			fmt.Println(" - clr: 清除冗余数据")
			fmt.Println(" - src: 优化并导出数据文件")
			fmt.Println(" - help: 帮助")
			fmt.Println(" - exit: 退出")
			continue
		} else if text == "exit" {
			return
		} else if text == "src" {
			if err := db.Source("../source/db01"); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("导出成功")
			}
			continue
		} else if text == "clr" {
			if err := db.Clear(); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("冗余数据清理成功")
			}
			continue
		} else if text == "getall" {
			ks, vs, err := db.GetAll()
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("数据条数:", len(ks))
				for i := 0; i < len(ks); i++ {
					fmt.Println(" - " + ks[i] + " - " + vs[i])
				}
			}
			continue
		} else if text == "getkeys" {
			fmt.Println(db.GetKeys())
			continue
		} else if len(text) > 3 && text[3] == ' ' {
			switch text[:3] {
			case "set":
				s := strings.TrimSpace(text[4:])
				for i := 0; i < len(s); i++ {
					if s[i] == ' ' {
						if err := db.Set(s[:i], strings.TrimSpace(s[i:])); err != nil {
							fmt.Println(err)
						}
						break
					}
					if i == len(s)-1 {
						fmt.Println("输入格式错误")
						fmt.Println(" - set: set [key] [value]")
						break
					}
				}
			case "get":
				key := strings.TrimSpace(text[4:])
				s := ""
				if err := db.Get(&s, key); err != nil {
					fmt.Println(err)
				}
				fmt.Println(s)
			case "del":
				key := strings.TrimSpace(text[4:])
				if err := db.Del(key); err != nil {
					fmt.Println(err)
				}
			default:
				fmt.Println("无效的输入,查看所有命令请使用help")
			}
			continue
		} else if len(text) > 6 && text[:7] == "getjson" {
			if text[7] != ' ' {
				fmt.Println("输入格式错误")
				fmt.Println(" - getjson: getjson [key] ,获取key对应数据的json格式")
			}
			key := strings.TrimSpace(text[8:])
			if b, err := db.GetJson(key); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(string(b))
			}
			continue
		} else {
			fmt.Println("无效的输入,查看所有命令请使用help")
		}
	}
}
