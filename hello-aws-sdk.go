package main

// https://github.com/markcallen/snssqs/blob/master/create.js

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)


var logger *log.Logger

var topicArn string
var queueURL string
var queueArn string

var snsClient *sns.SNS
var sqsClient *sqs.SQS

func getMessages() {
	maxNumberOfMessages := int64(10)
	for receiveMessageOutcome, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{QueueUrl: &queueURL, MaxNumberOfMessages: &maxNumberOfMessages}); err == nil; {
		for _, message := range receiveMessageOutcome.Messages {
			logger.Println("Message received: ", *message.Body)
			_, err := sqsClient.DeleteMessage(&sqs.DeleteMessageInput{QueueUrl: &queueURL, ReceiptHandle: message.ReceiptHandle})
			if err != nil {
				logger.Println("Error", err)
				return
			}
		}
	}
}

func main() {
	logger = log.New(os.Stdout, "", 0)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	// Create a SNS service client.
	snsClient = sns.New(sess)
	topicOutcome, err := snsClient.CreateTopic(&sns.CreateTopicInput{Name: aws.String("demo")})
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	topicArn = *topicOutcome.TopicArn

	// Create a SQS service client.
	sqsClient = sqs.New(sess)
	queueOutcome, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String("demo")})
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	queueURL = *queueOutcome.QueueUrl

	queueAttributesOutcome, err := sqsClient.GetQueueAttributes(
		&sqs.GetQueueAttributesInput{QueueUrl: &queueURL, AttributeNames: []*string{aws.String("QueueArn")}})
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	queueArn = *queueAttributesOutcome.Attributes["QueueArn"]

	_, err = snsClient.Subscribe(
		&sns.SubscribeInput{TopicArn: &topicArn, Protocol: aws.String("sqs"), Endpoint: &queueArn})
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	attributes := map[string]interface{}{
		"Version": "2008-10-17",
		"Id":      queueArn + "/SQSDefaultPolicy",
		"Statement": []interface{}{map[string]interface{}{
			"Sid":       "Sid" + strconv.FormatInt(time.Now().UnixNano()/(int64(time.Millisecond)/int64(time.Nanosecond)), 10),
			"Effect":    "Allow",
			"Principal": map[string]interface{}{"AWS": "*"},
			"Action":    "SQS:SendMessage",
			"Resource":  queueArn,
			"Condition": map[string]interface{}{"ArnEquals": map[string]interface{}{"aws:SourceArn": topicArn}}}}}

	jsonData, err := json.Marshal(attributes)
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	_, err = sqsClient.SetQueueAttributes(
		&sqs.SetQueueAttributesInput{QueueUrl: &queueURL, Attributes: map[string]*string{"Policy": aws.String(string(jsonData))}})
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	go getMessages()

	for i := 0; i < 100; i++ {
		message := "message: " + strconv.FormatInt(int64(i), 10)
		snsClient.Publish(&sns.PublishInput{TopicArn: &topicArn, Message: &message})
	}

}
