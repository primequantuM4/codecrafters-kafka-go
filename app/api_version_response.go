package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
)

type ResponseBody struct {
	CorrelationId  int32
	ErrorCode      int16
	NumOfApiKeys   int8
	Versions       []ApiVersion
	ThrottleTimeMs int32
}

func (rb *ResponseBody) Encode(buff *bytes.Buffer) {
	writeBytes(rb.CorrelationId, buff)
	writeBytes(rb.ErrorCode, buff)

	writeBytes(rb.NumOfApiKeys+1, buff)

	for _, version := range rb.Versions {
		version.Encode(buff)
	}

	writeBytes(rb.ThrottleTimeMs, buff)
	writeBytes(TAG_BUFFER, buff)
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (av *ApiVersion) Encode(buff *bytes.Buffer) {
	writeBytes(av.ApiKey, buff)
	writeBytes(av.MinVersion, buff)
	writeBytes(av.MaxVersion, buff)
	writeBytes(TAG_BUFFER, buff)
}

func constructResponse(correlationId int32) ResponseBody {
	responseBody := ResponseBody{
		CorrelationId:  correlationId,
		ErrorCode:      NO_ERROR_CODE,
		NumOfApiKeys:   2,
		ThrottleTimeMs: 0,
	}

	responseBody.Versions = append(
		responseBody.Versions,
		ApiVersion{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		ApiVersion{ApiKey: 1, MinVersion: 0, MaxVersion: 16},
	)

	return responseBody
}

func sendSuccessResponse(conn net.Conn, correlationId int32) {
	responseBody := constructResponse(correlationId)
	buff := new(bytes.Buffer)

	responseBody.Encode(buff)

	sentBuffer := prependLength(buff)
	_, err := conn.Write(sentBuffer)

	if err != nil {
		fmt.Println("Could not send the correlation id [", err.Error(), "]")
		os.Exit(1)
	}
}
