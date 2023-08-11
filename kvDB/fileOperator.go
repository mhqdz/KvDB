package kvDB

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Data Data文件设置
type Data struct {
	Path    string `yaml:"path"`
	DbName  string `yaml:"dbName"`
	MaxSize int    `yaml:"maxSize"`
}

// Conf 读取 config/conf.yml的结果
type Conf struct {
	Data Data `yaml:"data"`
}

// DefaultConf Conf默认配置
var DefaultConf Conf = Conf{
	Data: Data{
		Path:    "../data/",
		DbName:  "db01",
		MaxSize: 4,
	},
}

// InitConf 读取配置 读不到就用默认的
func InitConf() Conf {
	if _, err := os.Stat("../config/conf.yml"); err != nil {
		fmt.Println("配置文件不存在:", err, "使用默认配置")
		return DefaultConf
	}

	yamlData, err := os.ReadFile("../config/conf.yml")
	if err != nil {
		fmt.Println("打开配置文件错误:", err, "使用默认配置")
		return DefaultConf
	}

	var conf *Conf
	if err := yaml.Unmarshal(yamlData, &conf); err != nil {
		fmt.Println("读取配置文件错误:", err, "使用默认配置")
		return DefaultConf
	}

	return *conf
}

// DBMaster 数据库管理者 使用它可以调用 get/set/del等方法
type DBMaster struct {
	// 数据文件
	dataFile *os.File
	// sql语句历史文件
	sqlFile *os.File
	// hash存储 key和数据地址 []int{偏移量,长度}
	m map[string][2]int
}

// InitDBMaster 根据配置文件创建DBMaster Conf通过InitConf()创建
func InitDBMaster(conf Conf) (*DBMaster, error) {
	var master DBMaster
	filePath := conf.Data.Path + conf.Data.DbName
	dataFile, err := os.OpenFile(filePath+".mhD", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	master.dataFile = dataFile

	sqlFile, err := os.OpenFile(filePath+".mhI", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	master.sqlFile = sqlFile
	master.m = make(map[string][2]int)
	if err = master.SqlFileRead(); err != nil {
		return &master, err
	}
	return &master, nil
}

// Close 关闭文件
func (master *DBMaster) Close() error {
	if err := master.dataFile.Close(); err != nil {
		return err
	}
	return master.sqlFile.Close()
}

// GetJson Get key 返回json的byte数组
func (master *DBMaster) GetJson(key string) ([]byte, error) {
	if v, ok := master.m[key]; ok {
		buf := make([]byte, v[1])
		_, err := master.dataFile.ReadAt(buf, int64(v[0]))
		if err != nil {
			return nil, &masterErr{
				msg: "读取数据失败:",
				Err: err,
			}
		}
		return DataUnMarshalToJson(buf)
	}
	return nil, &masterErr{msg: "get key失败,key不存在"}
}

// Get Get key 把数据返回到传入的dst指针中
func (master *DBMaster) Get(dst any, key string) error {
	if v, ok := master.m[key]; ok {
		buf := make([]byte, v[1])
		_, err := master.dataFile.ReadAt(buf, int64(v[0]))
		if err != nil {
			return &masterErr{
				msg: "读取数据失败:",
				Err: err,
			}
		}
		return DataUnMarshal(dst, buf)
	}
	return &masterErr{msg: "get key失败,key不存在"}
}

// doSetSql 实现 set 传入key 以及对应的key对应的data文件中的偏移量和长度
func (master *DBMaster) doSetSql(key string, offset, len int) error {
	master.m[key] = [2]int{offset, len}
	_, err := master.sqlFile.WriteString("1" + string(encodeData([]byte(key))) + "*" + strconv.Itoa(offset) + "*" + strconv.Itoa(len) + "\n")
	return &masterErr{
		msg: "set sql写入出错",
		Err: err,
	}
}

// Set Set key = value
func (master *DBMaster) Set(key string, value any) error {
	if b, len, err := DataMarshal(value); err == nil {
		stat, err := master.dataFile.Stat()
		if err != nil {
			return &masterErr{
				msg: "读取文件数据时出错:",
				Err: err,
			}
		}
		if _, err = master.dataFile.Write(b); err == nil {
			master.doSetSql(key, int(stat.Size()), len)
		} else {
			return &masterErr{
				msg: "写入数据文件时出错:",
				Err: err,
			}
		}
	} else {
		return &masterErr{
			msg: "set sql编码时出错:",
			Err: err,
		}
	}

	return nil
}

// doDelSql 实现 Del key
func (master *DBMaster) doDelSql(key string) error {
	if _, ok := master.m[key]; ok {
		delete(master.m, key)

		_, err := master.sqlFile.WriteString("0" + string(encodeData([]byte(key))) + "\n")
		if err == nil {
			return nil
		}
		return &masterErr{
			msg: "del sql写入出错",
			Err: err,
		}
	}
	// 如果key不存在 不视为一种错误 但也不需要写入命令
	return nil
}

// Del del key delete的没有那么多错误要处理 为了和Set格式对齐写了这个多余的方法
func (master *DBMaster) Del(key string) error {
	return master.doDelSql(key)
}

// SqlFileRead 从索引文件中读取到map里
func (master *DBMaster) SqlFileRead() error {
	stat, err := master.sqlFile.Stat()
	if err != nil {
		return &masterErr{
			msg: "获取文件状态失败:",
			Err: err,
		}
	}

	buf := make([]byte, stat.Size())
	_, err = master.sqlFile.Read(buf)

	if err != nil {
		return &masterErr{
			msg: "读取sql文件失败:",
			Err: err,
		}
	}

	// todo 可以优化 倒序的读 每次读之后判断 是否存在key如果存在 就可以不执行
	l := 0
	for i := 0; i < len(buf); i++ {
		if buf[i] != '\n' {
			continue
		}
		if buf[l] == '0' {
			key, err := decodeData(buf[l+1 : i])
			if err != nil {
				return &masterErr{
					msg: "sql文件解码出错",
					Err: err,
				}
			}
			delete(master.m, string(key))
		} else if buf[l] == '1' {
			// flag1 记录第一个'*'符号的位置
			flag1 := -1
			var key []byte
			for j := l + 1; j < i+1; j++ {
				if buf[j] != '*' {
					continue
				}
				if flag1 < 0 {
					key, err = decodeData(buf[l+1 : j])
					if err != nil {
						return &masterErr{
							msg: "sql文件解码出错",
							Err: err,
						}
					}
					flag1 = j
					continue
				}
				offset, err := strconv.Atoi(string(buf[flag1+1 : j]))
				if err != nil {
					return &masterErr{
						msg: "offset转int失败,str:" + string(buf[flag1+1:j]) + "err:",
						Err: err,
					}
				}
				len, err := strconv.Atoi(string(buf[j+1 : i]))
				if err != nil {
					return &masterErr{
						msg: "len转int失败,str:" + string(buf[flag1+1:j]) + "err:",
						Err: err,
					}
				}
				master.m[string(key)] = [2]int{offset, len}
				break
			}
		}
		l = i + 1
	}

	return err
}

// GetKeys 把所有的key 集中到一个数组返回
func (master *DBMaster) GetKeys() []string {
	keys := []string{}
	for k := range master.m {
		keys = append(keys, k)
	}
	return keys
}

// GetAll 返回所有键与值 key-json(value) 有些json不能解码成string 不如直接返回json
func (master *DBMaster) GetAll() ([]string, []string, error) {
	keys := []string{}
	values := []string{}
	for k := range master.m {
		keys = append(keys, k)
		b, err := master.GetJson(k)
		if err != nil {
			return []string{}, []string{}, err
		}
		values = append(values, string(b))
	}
	return keys, values, nil
}

// Source 优化并导出数据
func (master *DBMaster) Source(filePath string) error {
	if find := strings.Contains(filePath, "/") || strings.Contains(filePath, "\\"); find {
		pathMkdir(filePath)
	} else {
		filePath = "./" + filePath
	}

	dataFile, err := os.OpenFile(filePath+".mhD", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	// 如果文件原来有数据清空原有数据
	dataFile.Truncate(0)

	sqlFile, err := os.OpenFile(filePath+".mhI", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	sqlFile.Truncate(0)

	sqlStr := ""
	newDatas := []byte{}
	for k := range master.m {
		data := make([]byte, master.m[k][1])
		offset := len(newDatas)
		master.dataFile.ReadAt(data, int64(master.m[k][0]))
		newDatas = append(newDatas, data...)
		// WriteString("1" + string(encodeData([]byte(key))) + "*" + strconv.Itoa(offset) + "*" + strconv.Itoa(len) + "\n")
		sqlStr += "1" + string(encodeData([]byte(k))) + "*" + strconv.Itoa(offset) + "*" + strconv.Itoa(master.m[k][1]) + "\n"
	}

	dataFile.Write(newDatas)
	sqlFile.WriteString(sqlStr)

	return nil
}

// todo
// 实现思路 根据map里的数据 写两个新的文件 然后替换掉 .mhD 和 .mhI两个文件
// Clear 去除两个data文件中没有用的东西
func (master *DBMaster) Clear() error {
	sqlStr := ""
	newDatas := []byte{}
	for k := range master.m {
		data := make([]byte, master.m[k][1])
		offset := len(newDatas)
		master.dataFile.ReadAt(data, int64(master.m[k][0]))
		newDatas = append(newDatas, data...)
		// WriteString("1" + string(encodeData([]byte(key))) + "*" + strconv.Itoa(offset) + "*" + strconv.Itoa(len) + "\n")
		sqlStr += "1" + string(encodeData([]byte(k))) + "*" + strconv.Itoa(offset) + "*" + strconv.Itoa(master.m[k][1]) + "\n"
	}

	// 直接动源文件的做法 错误不好处理 而且一旦发生错误 可能数据库就没了
	// 最好是像事务一样处理下面的四个语句 出错回滚 但我不知道怎么做
	master.dataFile.Truncate(0)
	master.dataFile.Write(newDatas)
	master.sqlFile.Truncate(0)
	master.sqlFile.WriteString(sqlStr)

	return nil
}
