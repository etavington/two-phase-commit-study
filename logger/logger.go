package logger

import (
	"io"
	"log"
	"os"
)

var f, _ = os.OpenFile("2pc.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
var multi = io.Writer(f)
var Logger = log.New(multi, "2pc ", log.LstdFlags)
