package main

import (
	"bytes"
	"net"
	"os"
)

// ERROR CODE TYPES
const NO_ERROR_CODE = int16(0)
const UNSUPPORTED_VERSION = int16(35)
const UNKNOWN_TOPIC = int16(100)

func sendErrorResponse(conn net.Conn, correlationId int32) {
	buff := new(bytes.Buffer)
	responseBody := ResponseBody{
		CorrelationId: correlationId,
		ErrorCode:     UNSUPPORTED_VERSION,
	}

	writeBytes(responseBody.CorrelationId, buff)
	writeBytes(responseBody.ErrorCode, buff)

	sentBuffer := prependLength(buff)
	conn.Write(sentBuffer)
	os.Exit(0)

}
