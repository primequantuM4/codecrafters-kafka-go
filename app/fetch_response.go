package main

import (
	"bytes"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
)

type FetchResponse struct {
	CorrelationId  int32
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Responses      []Response
}

var topics = make(map[uuid.UUID]bool)

func (fr *FetchResponse) Encode(buff *bytes.Buffer) {

	//Response Header
	writeBytes(fr.CorrelationId, buff)
	writeBytes(TAG_BUFFER, buff)
	//Response Body
	writeBytes(fr.ThrottleTimeMs, buff)
	writeBytes(fr.ErrorCode, buff)
	writeBytes(fr.SessionId, buff)
	writeBytes(int8(len(fr.Responses)+1), buff)

	for _, response := range fr.Responses {
		response.Encode(buff)
	}

	writeBytes(TAG_BUFFER, buff)
}

type Response struct {
	TopicId    uuid.UUID
	Partitions []PartitionsResponse
}

func (r *Response) Encode(buff *bytes.Buffer) {
	writeBytes(r.TopicId, buff)
	writeBytes(int8(len(r.Partitions)+1), buff)

	for _, parition := range r.Partitions {
		parition.Encode(buff)
	}

	writeBytes(TAG_BUFFER, buff)
}

type PartitionsResponse struct {
	ParitionIndex        int32
	ErrorCode            int16
	HighWaterMark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PrefferedReadReplica int32
	Records              []byte
}

func (pr *PartitionsResponse) Encode(buff *bytes.Buffer) {
	writeBytes(pr.ParitionIndex, buff)
	writeBytes(pr.ErrorCode, buff)
	writeBytes(pr.HighWaterMark, buff)
	writeBytes(pr.LastStableOffset, buff)
	writeBytes(pr.LogStartOffset, buff)
	writeBytes(int8(len(pr.AbortedTransactions)+1), buff)

	for _, abortedTransaction := range pr.AbortedTransactions {
		abortedTransaction.Encode(buff)
	}

	writeBytes(pr.PrefferedReadReplica, buff)
	writeBytes(int8(len(pr.Records)+1), buff)
	writeBytes(pr.Records, buff)

	writeBytes(TAG_BUFFER, buff)
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
}

func (at *AbortedTransaction) Encode(buff *bytes.Buffer) {
	writeBytes(at.ProducerId, buff)
	writeBytes(at.FirstOffset, buff)
	writeBytes(TAG_BUFFER, buff)
}

func constructFetchResponse(correlationId int32, fetchRequest FetchRequest) FetchResponse {
	fetchResponse := FetchResponse{
		CorrelationId:  correlationId,
		ThrottleTimeMs: 0,
		ErrorCode:      NO_ERROR_CODE,
		SessionId:      0,
	}

	if fetchRequest.TopicNum-1 > 0 {
		var errorCode int16

		if fetchRequest.Topics[0].TopicId.String()[14] != '4' {
			errorCode = UNKNOWN_TOPIC
		} else {
			errorCode = NO_ERROR_CODE
		}

		fetchResponse.Responses = append(fetchResponse.Responses,
			Response{
				TopicId: fetchRequest.Topics[0].TopicId,
				Partitions: []PartitionsResponse{
					{
						ParitionIndex:        0,
						ErrorCode:            errorCode,
						HighWaterMark:        0,
						LastStableOffset:     0,
						LogStartOffset:       0,
						AbortedTransactions:  []AbortedTransaction{},
						PrefferedReadReplica: 0,
						Records:              []byte{},
					},
				}},
		)
	}

	return fetchResponse
}

func sendFetchResponse(conn net.Conn, correlationId int32, fetchRequest FetchRequest) {
	fetchResponse := constructFetchResponse(correlationId, fetchRequest)
	buff := new(bytes.Buffer)

	fetchResponse.Encode(buff)

	sentBuffer := prependLength(buff)
	_, err := conn.Write(sentBuffer)

	if err != nil {
		fmt.Println("Could not send the fetch response [", err.Error(), "]")
		os.Exit(1)
	}
}
