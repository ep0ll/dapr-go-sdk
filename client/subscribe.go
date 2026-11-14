/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/go-sdk/service/common"
)

type SubscriptionHandleFunction func(event *common.TopicEvent) common.SubscriptionResponseStatus

// BulkSubscribeOptions configures batched delivery for a streaming subscription.
type BulkSubscribeOptions struct {
	Enabled            bool
	MaxMessagesCount   int32
	MaxAwaitDurationMs int32
}

type SubscriptionOptions struct {
	PubsubName      string
	Topic           string
	DeadLetterTopic *string
	Metadata        map[string]string
	BulkSubscribe   *BulkSubscribeOptions
}

type Subscription struct {
	ctx        context.Context
	stream     pb.Dapr_SubscribeTopicEventsAlpha1Client
	pending    []*SubscriptionMessage
	generation uint64

	// lock locks concurrent writes to subscription stream.
	lock   sync.Mutex
	closed atomic.Bool

	createStream func(ctx context.Context, opts SubscriptionOptions) (pb.Dapr_SubscribeTopicEventsAlpha1Client, error)
	opts         SubscriptionOptions
}

type SubscriptionMessage struct {
	*common.TopicEvent
	sub              *Subscription
	ackID            string
	streamGeneration uint64
}

func (c *GRPCClient) Subscribe(ctx context.Context, opts SubscriptionOptions) (*Subscription, error) {
	stream, err := c.subscribeInitialRequest(ctx, opts)
	if err != nil {
		return nil, err
	}

	s := &Subscription{
		ctx:          ctx,
		stream:       stream,
		createStream: c.subscribeInitialRequest,
		opts:         opts,
	}

	return s, nil
}

func (c *GRPCClient) SubscribeWithHandler(ctx context.Context, opts SubscriptionOptions, handler SubscriptionHandleFunction) (func() error, error) {
	s, err := c.Subscribe(ctx, opts)
	if err != nil {
		return nil, err
	}

	go func() {
		defer s.Close()

		for {
			msg, err := s.Receive()
			if err != nil {
				if !s.closed.Load() {
					logger.Printf("Error receiving messages from subscription pubsub=%s topic=%s, closing subscription: %s",
						opts.PubsubName, opts.Topic, err)
				}
				return
			}

			go func() {
				if err := msg.respondStatus(handler(msg.TopicEvent)); err != nil {
					logger.Printf("Error responding to topic with event status pubsub=%s topic=%s message_id=%s: %s",
						opts.PubsubName, opts.Topic, msg.ID, err)
				}
			}()
		}
	}()

	return s.Close, nil
}

func (s *Subscription) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return errors.New("subscription already closed")
	}

	return s.stream.CloseSend()
}

func (s *Subscription) Receive() (*SubscriptionMessage, error) {
	if len(s.pending) > 0 {
		message := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		return message, nil
	}

	for {
		s.lock.Lock()
		stream := s.stream
		generation := s.generation
		s.lock.Unlock()
		if stream == nil {
			return nil, errors.New("subscription stream is not initialized")
		}

		resp, err := stream.Recv()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil, errors.New("subscription context closed")
			default:
				// proceed to check the gRPC status error
			}

			st, ok := status.FromError(err)
			if !ok {
				return nil, err
			}

			switch st.Code() {
			case codes.Unavailable, codes.Unknown:
				logger.Printf("gRPC error while reading from stream: %s (code=%v)", st.Message(), st.Code())
				// Any buffered entries belong to the stream that just failed.
				// They must not be delivered (or acknowledged) on its replacement.
				s.pending = nil
				if s.closed.Load() {
					return nil, errors.New("subscription is permanently closed; cannot reconnect")
				}
				if err := s.closeStreamOnly(); err != nil {
					logger.Printf("error closing current stream: %v", err)
				}

				newStream, nerr := s.createStream(s.ctx, s.opts)
				if nerr != nil {
					return nil, fmt.Errorf("re-subscribe failed: %w", nerr)
				}

				s.lock.Lock()
				s.stream = newStream
				s.generation++
				s.lock.Unlock()
				continue

			case codes.Canceled:
				return nil, errors.New("stream canceled")
			default:
				return nil, fmt.Errorf("subscription recv error: %w", err)
			}
		}

		if event := resp.GetEventMessage(); event != nil {
			if len(resp.GetEventMessages()) > 0 {
				return nil, errors.New("streaming response contains both a single event and bulk events")
			}
			return &SubscriptionMessage{
				sub: s, TopicEvent: topicEventFromSingle(event), ackID: event.GetId(), streamGeneration: generation,
			}, nil
		}

		if events := resp.GetEventMessages(); len(events) > 0 {
			messages := make([]*SubscriptionMessage, 0, len(events))
			for _, event := range events {
				if event == nil || event.GetEntry() == nil || event.GetDeliveryId() == "" {
					return nil, errors.New("streaming bulk response contains an invalid entry or empty delivery ID")
				}
				topicEvent, err := topicEventFromBulk(event.GetEntry(), s.opts)
				if err != nil {
					return nil, err
				}
				messages = append(messages, &SubscriptionMessage{
					sub: s, TopicEvent: topicEvent, ackID: event.GetDeliveryId(), streamGeneration: generation,
				})
			}
			s.pending = append(s.pending, messages[1:]...)
			return messages[0], nil
		}

		if resp.GetInitialResponse() != nil {
			continue
		}
		return nil, errors.New("streaming response contains no event")
	}
}

func topicEventFromSingle(event *pb.TopicEventRequest) *common.TopicEvent {
	var extensions map[string]interface{}
	if event.GetExtensions() != nil {
		extensions = event.GetExtensions().AsMap()
	}
	return &common.TopicEvent{
		ID: event.GetId(), Source: event.GetSource(), Type: event.GetType(),
		SpecVersion: event.GetSpecVersion(), DataContentType: event.GetDataContentType(),
		Data: decodeSubscriptionData(event.GetData(), event.GetDataContentType()), RawData: event.GetData(),
		Topic: event.GetTopic(), PubsubName: event.GetPubsubName(), Extensions: extensions,
	}
}

func topicEventFromBulk(event *pb.TopicEventBulkRequestEntry, opts SubscriptionOptions) (*common.TopicEvent, error) {
	if event == nil {
		return nil, errors.New("streaming bulk entry is nil")
	}
	if cloudEvent := event.GetCloudEvent(); cloudEvent != nil {
		data := cloudEvent.GetData()
		var extensions map[string]interface{}
		if cloudEvent.GetExtensions() != nil {
			extensions = cloudEvent.GetExtensions().AsMap()
		}
		return &common.TopicEvent{
			ID: cloudEvent.GetId(), EntryID: event.GetEntryId(), Source: cloudEvent.GetSource(),
			Type: cloudEvent.GetType(), SpecVersion: cloudEvent.GetSpecVersion(),
			DataContentType: cloudEvent.GetDataContentType(),
			Data: decodeSubscriptionData(data, cloudEvent.GetDataContentType()), RawData: data,
			Topic: opts.Topic, PubsubName: opts.PubsubName, Metadata: event.GetMetadata(),
			Extensions: extensions,
		}, nil
	}
	if data := event.GetBytes(); data != nil {
		return &common.TopicEvent{
			ID: event.GetEntryId(), EntryID: event.GetEntryId(),
			DataContentType: event.GetContentType(),
			Data: decodeSubscriptionData(data, event.GetContentType()), RawData: data,
			Topic: opts.Topic, PubsubName: opts.PubsubName, Metadata: event.GetMetadata(),
		}, nil
	}
	return nil, fmt.Errorf("streaming bulk entry %q has no event payload", event.GetEntryId())
}

func decodeSubscriptionData(data []byte, contentType string) any {
	value := any(data)
	if len(data) == 0 {
		return value
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return value
	}
	var decoded any
	switch mediaType {
	case "application/json":
		if json.Unmarshal(data, &decoded) == nil {
			return decoded
		}
	case "text/plain":
		return string(data)
	default:
		if strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json") {
			if json.Unmarshal(data, &decoded) == nil {
				return decoded
			}
		}
	}
	return value
}

func (s *SubscriptionMessage) Success() error {
	return s.respond(pb.TopicEventResponse_SUCCESS)
}

func (s *SubscriptionMessage) Retry() error {
	return s.respond(pb.TopicEventResponse_RETRY)
}

func (s *SubscriptionMessage) Drop() error {
	return s.respond(pb.TopicEventResponse_DROP)
}

func (s *SubscriptionMessage) respondStatus(status common.SubscriptionResponseStatus) error {
	var statuspb pb.TopicEventResponse_TopicEventResponseStatus
	switch status {
	case common.SubscriptionResponseStatusSuccess:
		statuspb = pb.TopicEventResponse_SUCCESS
	case common.SubscriptionResponseStatusRetry:
		statuspb = pb.TopicEventResponse_RETRY
	case common.SubscriptionResponseStatusDrop:
		statuspb = pb.TopicEventResponse_DROP
	default:
		return fmt.Errorf("unknown status, expected one of %s, %s, %s: %s",
			common.SubscriptionResponseStatusSuccess, common.SubscriptionResponseStatusRetry,
			common.SubscriptionResponseStatusDrop, status)
	}

	return s.respond(statuspb)
}

func (s *SubscriptionMessage) respond(status pb.TopicEventResponse_TopicEventResponseStatus) error {
	s.sub.lock.Lock()
	defer s.sub.lock.Unlock()

	if s.streamGeneration != s.sub.generation {
		return errors.New("cannot acknowledge a message received on a replaced subscription stream")
	}
	return s.sub.stream.Send(&pb.SubscribeTopicEventsRequestAlpha1{
		SubscribeTopicEventsRequestType: &pb.SubscribeTopicEventsRequestAlpha1_EventProcessed{
			EventProcessed: &pb.SubscribeTopicEventsRequestProcessedAlpha1{
				Id: s.ackID,
				Status: &pb.TopicEventResponse{Status: status},
			},
		},
	})
}

func bulkSubscribeConfig(opts *BulkSubscribeOptions) *pb.BulkSubscribeConfig {
	if opts == nil {
		return nil
	}
	return &pb.BulkSubscribeConfig{
		Enabled: opts.Enabled, MaxMessagesCount: opts.MaxMessagesCount,
		MaxAwaitDurationMs: opts.MaxAwaitDurationMs,
	}
}

func (c *GRPCClient) subscribeInitialRequest(ctx context.Context, opts SubscriptionOptions) (pb.Dapr_SubscribeTopicEventsAlpha1Client, error) {
	if len(opts.PubsubName) == 0 {
		return nil, errors.New("pubsub name required")
	}

	if len(opts.Topic) == 0 {
		return nil, errors.New("topic required")
	}

	stream, err := c.protoClient.SubscribeTopicEventsAlpha1(ctx)
	if err != nil {
		return nil, err
	}

	err = stream.Send(&pb.SubscribeTopicEventsRequestAlpha1{
		SubscribeTopicEventsRequestType: &pb.SubscribeTopicEventsRequestAlpha1_InitialRequest{
			InitialRequest: &pb.SubscribeTopicEventsRequestInitialAlpha1{
				PubsubName: opts.PubsubName, Topic: opts.Topic,
				Metadata: opts.Metadata, DeadLetterTopic: opts.DeadLetterTopic,
				BulkSubscribe: bulkSubscribeConfig(opts.BulkSubscribe),
			},
		},
	})
	if err != nil {
		return nil, errors.Join(err, stream.CloseSend())
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, errors.Join(err, stream.CloseSend())
	}

	switch resp.GetSubscribeTopicEventsResponseType().(type) {
	case *pb.SubscribeTopicEventsResponseAlpha1_InitialResponse:
	default:
		return nil, fmt.Errorf("unexpected initial response from server : %v", resp)
	}

	return stream, nil
}

func (s *Subscription) closeStreamOnly() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.stream != nil {
		return s.stream.CloseSend()
	}
	return nil
}
