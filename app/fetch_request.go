package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

type FetchRequest struct {
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionId       int32
	SessionEpoch    int32
	TopicNum        int8
	Topics          []TopicRequest
	ForgottenTopics []ForgottenTopic
	RackId          []byte
}

func (fr *FetchRequest) Parse(reader *bytes.Reader) {
	binary.Read(reader, binary.BigEndian, &fr.MaxWaitMs)
	binary.Read(reader, binary.BigEndian, &fr.MinBytes)
	binary.Read(reader, binary.BigEndian, &fr.MaxBytes)
	binary.Read(reader, binary.BigEndian, &fr.IsolationLevel)
	binary.Read(reader, binary.BigEndian, &fr.SessionId)
	binary.Read(reader, binary.BigEndian, &fr.SessionEpoch)
	binary.Read(reader, binary.BigEndian, &fr.TopicNum)

	for i := 0; i < int(fr.TopicNum-1); i++ {
		var topic TopicRequest
		topic.Parse(reader)
		fr.Topics = append(fr.Topics, topic)

	}

	for _, forgottenTopic := range fr.ForgottenTopics {
		forgottenTopic.Parse(reader)
	}

	binary.Read(reader, binary.BigEndian, &fr.RackId)
	binary.Read(reader, binary.BigEndian, TAG_BUFFER)

	fmt.Printf("Data successfully parsed: %+v\n", fr)

}

type TopicRequest struct {
	TopicId       uuid.UUID
	PartitionsNum int8
	Partitions    []PartitionsRequest
}

func (tr *TopicRequest) Parse(reader *bytes.Reader) {
	binary.Read(reader, binary.BigEndian, &tr.TopicId)
	binary.Read(reader, binary.BigEndian, &tr.PartitionsNum)
	for _, partition := range tr.Partitions {
		partition.Parse(reader)
	}
	binary.Read(reader, binary.BigEndian, TAG_BUFFER)
	fmt.Printf("Topic Request has been parsed %+v\n", tr)
}

type PartitionsRequest struct {
	Parition           int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	ParitionMaxBytes   int32
	ForgottenTopicsNum int8
}

func (pr *PartitionsRequest) Parse(reader *bytes.Reader) {
	binary.Read(reader, binary.BigEndian, &pr.Parition)
	binary.Read(reader, binary.BigEndian, &pr.CurrentLeaderEpoch)
	binary.Read(reader, binary.BigEndian, &pr.FetchOffset)
	binary.Read(reader, binary.BigEndian, &pr.LastFetchedEpoch)
	binary.Read(reader, binary.BigEndian, &pr.LogStartOffset)
	binary.Read(reader, binary.BigEndian, &pr.ParitionMaxBytes)
	binary.Read(reader, binary.BigEndian, &pr.ForgottenTopicsNum)
}

type ForgottenTopic struct {
	TopicId    uuid.UUID
	Partitions int32
}

func (ft *ForgottenTopic) Parse(reader *bytes.Reader) {
	binary.Read(reader, binary.BigEndian, &ft.TopicId)
	binary.Read(reader, binary.BigEndian, &ft.Partitions)
}

func ParseRequest(reader *bytes.Reader) FetchRequest {
	var fetchRequest FetchRequest
	fmt.Println("EZI GEBICHALEW")
	fetchRequest.Parse(reader)

	fmt.Printf("Final data is %+v\n", fetchRequest)

	return fetchRequest
}
