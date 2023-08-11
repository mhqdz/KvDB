package kvDB

// masterErr 错误处理
type masterErr struct {
	msg string
	Err error
}

func (e *masterErr) Error() string {
	if e.Err == nil {
		return "errMsg:" + e.msg
	}
	return "errMsg:" + e.msg + e.Err.Error()
}
