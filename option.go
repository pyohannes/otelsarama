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
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const defaultTracerName = "go.opentelemetry.io/contrib/instrumentation/github.com/IBM/sarama/otelsarama"
const defaultMeterName = defaultTracerName

type config struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagators    propagation.TextMapPropagator

	Tracer trace.Tracer
	Meter  metric.Meter

	ServerAddress   string
	ServerPort      int
	ConsumerGroupID string
}

// newConfig returns a config with all Options set.
func newConfig(opts ...Option) config {
	cfg := config{
		Propagators:    otel.GetTextMapPropagator(),
		TracerProvider: otel.GetTracerProvider(),
		MeterProvider:  otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	cfg.Tracer = cfg.TracerProvider.Tracer(
		defaultTracerName,
		trace.WithInstrumentationVersion(Version()),
	)

	cfg.Meter = cfg.MeterProvider.Meter(
		defaultMeterName,
		metric.WithInstrumentationVersion(Version()),
	)

	return cfg
}

// Option interface used for setting optional config properties.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (fn optionFunc) apply(c *config) {
	fn(c)
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(cfg *config) {
		if provider != nil {
			cfg.TracerProvider = provider
		}
	})
}

// WithPropagators specifies propagators to use for extracting
// information from the HTTP requests. If none are specified, global
// ones will be used.
func WithPropagators(propagators propagation.TextMapPropagator) Option {
	return optionFunc(func(cfg *config) {
		if propagators != nil {
			cfg.Propagators = propagators
		}
	})
}

func WithBrokerAddresses(addresses []string) Option {
	return optionFunc(func(cfg *config) {
		if len(addresses) == 1 {
			uriParts := strings.Split(addresses[0], ":")

			cfg.ServerAddress = uriParts[0]

			if len(uriParts) > 1 {
				if iPort, err := strconv.Atoi(uriParts[1]); err == nil {
					cfg.ServerPort = iPort
				}
			}
		} else {
			cfg.ServerAddress = strings.Join(addresses, ";")
		}
	})
}

func WithConsumerGroup(groupID string) Option {
	return optionFunc(func(cfg *config) {
		cfg.ConsumerGroupID = groupID
	})
}
