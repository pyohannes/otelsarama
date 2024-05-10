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

	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)


type MessageProcessInstrumenter struct {
	cfg config

	metricProcessDuration metric.Float64Histogram
	defaultAttributes  []attribute.KeyValue
}

func NewMessageProcessInstrumenter(opts ...Option) MessageProcessInstrumenter {
	cfg := newConfig(opts...)

	processDuration, _ := cfg.Meter.Float64Histogram(
		"messaging.client.process.duration",
		metric.WithUnit("s"),
	)

	defaultAttributes := []attribute.KeyValue{
		semconv.MessagingSystem("kafka"),
		attribute.String("messaging.operation.name", "process"),
	}	
	if cfg.ServerAddress != "" {
		defaultAttributes = append(defaultAttributes, attribute.String("server.address", cfg.ServerAddress))
	}
	if cfg.ServerPort != 0 {
		defaultAttributes = append(defaultAttributes, attribute.Int("server.port", cfg.ServerPort))
	}

	return MessageProcessInstrumenter {
		cfg: cfg,
		metricProcessDuration: processDuration,
		defaultAttributes: defaultAttributes,
	}
}

type MessageProcessOperation struct {
	instrumenter MessageProcessInstrumenter

	err error 
	start time.Time
	span trace.Span
	topic string
	partition string
}

func (instrumenter *MessageProcessInstrumenter) NewProcessOperation(msg *sarama.ConsumerMessage) MessageProcessOperation {
		// Extract a span context from message to link.
		carrier := NewConsumerMessageCarrier(msg)
		parentSpanContext := instrumenter.cfg.Propagators.Extract(context.Background(), carrier)

		// Create a span.
		attrs := append(instrumenter.defaultAttributes,
			semconv.MessagingDestinationName(msg.Topic),
			attribute.String("messaging.message.id", strconv.FormatInt(msg.Offset, 10)),
			attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.Partition), 10)),
		)
		opts := []trace.SpanStartOption{
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithLinks(trace.LinkFromContext(parentSpanContext)),
		}
		_, span := instrumenter.cfg.Tracer.Start(parentSpanContext, fmt.Sprintf("%s process", msg.Topic), opts...)

	return MessageProcessOperation{
		span: span,
		topic: msg.Topic,
		partition: strconv.FormatInt(int64(msg.Partition), 10),
		start: time.Now(),
	}
}

func (msg *MessageProcessOperation) SetError(err error) {
	msg.err = err
}

func (msg *MessageProcessOperation) Stop() {
	msg.span.End()

	attrs := append(msg.instrumenter.defaultAttributes,
		semconv.MessagingDestinationName(msg.topic),
		attribute.String("messaging.destination.partition.id", msg.partition),
	)

	if msg.err != nil {
		attrs = append(attrs, attribute.String("error.type", msg.err.Error()))
	}

	// Add to our counter with an attribute
	msg.instrumenter.metricProcessDuration.Record(
		context.Background(), 
		time.Now().Sub(msg.start).Seconds(), 
		metric.WithAttributes(attrs...))
}
