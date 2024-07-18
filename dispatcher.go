// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelsarama

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/IBM/sarama"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type consumerMessagesDispatcher interface {
	Messages() <-chan *sarama.ConsumerMessage
}

type consumerMessagesDispatcherWrapper struct {
	d        consumerMessagesDispatcher
	messages chan *sarama.ConsumerMessage

	receiveDuration    metric.Float64Histogram
	consumedMessages   metric.Int64Counter
	defaultAttributes  []attribute.KeyValue

	cfg config
}

func newConsumerMessagesDispatcherWrapper(d consumerMessagesDispatcher, cfg config) *consumerMessagesDispatcherWrapper {
	receiveDuration, _ := cfg.Meter.Float64Histogram(
		"messaging.client.operation.duration",
		metric.WithUnit("s"),
	)

	consumedMessages, _ := cfg.Meter.Int64Counter(
		"messaging.client.consumed.messages",
	)

	defaultAttributes := []attribute.KeyValue{
		semconv.MessagingSystem("kafka"),
		attribute.String("messaging.operation.name", "receive"),
	}	
	if cfg.ServerAddress != "" {
		defaultAttributes = append(defaultAttributes, attribute.String("server.address", cfg.ServerAddress))
	}
	if cfg.ServerPort != 0 {
		defaultAttributes = append(defaultAttributes, attribute.Int("server.port", cfg.ServerPort))
	}
	if cfg.ConsumerGroupID != "" {
		defaultAttributes = append(defaultAttributes, attribute.String("messaging.consumer.group.name", cfg.ConsumerGroupID))
	}

	return &consumerMessagesDispatcherWrapper{
		d:        d,
		messages: make(chan *sarama.ConsumerMessage),
		receiveDuration: receiveDuration,
		consumedMessages: consumedMessages,
		defaultAttributes: defaultAttributes,
		cfg:      cfg,
	}
}

// Messages returns the read channel for the messages that are returned by
// the broker.
func (w *consumerMessagesDispatcherWrapper) Messages() <-chan *sarama.ConsumerMessage {
	return w.messages
}

func (w *consumerMessagesDispatcherWrapper) Run() {
	msgs := w.d.Messages()

	for msg := range msgs {
		start := time.Now()

		// Extract a span context from message to link.
		carrier := NewConsumerMessageCarrier(msg)
		parentSpanContext := w.cfg.Propagators.Extract(context.Background(), carrier)

		// Create a span.
		attrs := append(w.defaultAttributes,
			semconv.MessagingDestinationName(msg.Topic),
			attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.Partition), 10)),
		)
		spanAttrs := append(attrs, attribute.String("messaging.message.id", strconv.FormatInt(msg.Offset, 10)))

		opts := []trace.SpanStartOption{
			trace.WithAttributes(spanAttrs...),
			trace.WithLinks(trace.LinkFromContext(parentSpanContext)),
		}
		_, span := w.cfg.Tracer.Start(parentSpanContext, fmt.Sprintf("%s receive", msg.Topic), opts...)

		// Send messages back to user.
		w.messages <- msg

		span.End()

		// Add to our counter with an attribute
		w.receiveDuration.Record(context.Background(), time.Now().Sub(start).Seconds(), metric.WithAttributes(attrs...))
		w.consumedMessages.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	}
	close(w.messages)
}
