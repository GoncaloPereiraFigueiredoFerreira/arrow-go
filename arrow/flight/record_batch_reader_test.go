// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flight_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/internal/arrdata"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

func TestReaderIgnoresMetadataOnlyMessages(t *testing.T) {
	patterns := []struct {
		name        string
		leading     int
		interleaved bool
	}{
		{"no_heartbeats", 0, false},
		{"one_heartbeat", 1, false},
		{"startup_heartbeats", 5, false},
		{"interleaved_heartbeats", 5, true},
		{"long_startup_burst", 1024, false},
	}
	for _, pattern := range patterns {
		for _, schemaMode := range []string{"automatic", "expected", "delayed"} {
			t.Run(pattern.name+"/"+schemaMode, func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				records := arrdata.Records["primitives"]
				builder := array.NewRecordBuilder(alloc, records[0].Schema())
				empty := builder.NewRecordBatch()
				builder.Release()
				defer empty.Release()
				expected := []arrow.RecordBatch{records[0], empty, records[1]}
				stream := collectingFlightStreamWriter{}
				writer := flight.NewRecordWriter(&stream, ipc.WithSchema(records[0].Schema()), ipc.WithAllocator(alloc))
				for i, record := range expected {
					require.NoError(t, writer.WriteWithAppMetadata(record, []byte(fmt.Sprintf("batch-%d", i))))
				}
				require.NoError(t, writer.Close())
				stream.payloads[0].AppMetadata = []byte("schema")
				descriptor := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"dataset"}}
				updated := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"updated"}}
				input := metadataFlightStream{}
				for i := 0; i < pattern.leading; i++ {
					message := &flight.FlightData{AppMetadata: []byte("heartbeat")}
					if i == 0 {
						message.FlightDescriptor = descriptor
					}
					input.payloads = append(input.payloads, message)
				}
				if pattern.leading == 0 {
					stream.payloads[0].FlightDescriptor = descriptor
				}
				for i, payload := range stream.payloads {
					input.payloads = append(input.payloads, payload)
					if pattern.interleaved {
						input.payloads = append(input.payloads, &flight.FlightData{AppMetadata: []byte("heartbeat")})
						if i == 1 {
							input.payloads = append(input.payloads, &flight.FlightData{FlightDescriptor: updated})
						}
						input.payloads = append(input.payloads, &flight.FlightData{}, &flight.FlightData{AppMetadata: []byte("heartbeat")})
					}
				}
				options := []ipc.Option{ipc.WithAllocator(alloc)}
				switch schemaMode {
				case "expected":
					options = append(options, ipc.WithSchema(records[0].Schema()))
				case "delayed":
					options = append(options, ipc.WithDelayReadSchema(true))
				}
				reader, err := flight.NewRecordReader(&input, options...)
				require.NoError(t, err)
				defer reader.Release()
				require.True(t, records[0].Schema().Equal(reader.Schema()))
				require.Equal(t, []byte("schema"), reader.LatestAppMetadata())
				for i, record := range expected {
					require.True(t, reader.Next(), "missing record %d: %v", i, reader.Err())
					require.True(t, array.RecordEqual(record, reader.RecordBatch()), "record %d differs", i)
					expectedDescriptor := descriptor
					if pattern.interleaved && i > 0 {
						expectedDescriptor = updated
					}
					require.Equal(t, flight.StreamChunk{
						Data: reader.RecordBatch(), Desc: expectedDescriptor,
						AppMetadata: []byte(fmt.Sprintf("batch-%d", i)),
					}, reader.Chunk())
				}
				require.False(t, reader.Next())
				require.NoError(t, reader.Err())
				require.Nil(t, reader.LatestAppMetadata())
				require.Nil(t, reader.LatestFlightDescriptor())
				require.Equal(t, len(input.payloads), input.next)
			})
		}
	}
}

func TestReaderMetadataOnlyTermination(t *testing.T) {
	for _, terminal := range []struct {
		name string
		err  error
	}{
		{"eof", io.EOF},
		{"cancelled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
		{"stream_error", io.ErrUnexpectedEOF},
	} {
		for _, withSchema := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/schema=%t", terminal.name, withSchema), func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				input := metadataFlightStream{terminal: terminal.err}
				if withSchema {
					schema := arrdata.Records["primitives"][0].Schema()
					stream := collectingFlightStreamWriter{}
					writer := flight.NewRecordWriter(&stream, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
					require.NoError(t, writer.Write(arrdata.Records["primitives"][0]))
					require.NoError(t, writer.Close())
					input.payloads = append(input.payloads, stream.payloads[0])
				}
				for i := 0; i < 3; i++ {
					input.payloads = append(input.payloads, &flight.FlightData{AppMetadata: []byte("heartbeat")})
				}
				reader, err := flight.NewRecordReader(&input, ipc.WithAllocator(alloc))
				if !withSchema {
					require.ErrorIs(t, err, terminal.err)
					require.Nil(t, reader)
					return
				}
				require.NoError(t, err)
				defer reader.Release()
				require.False(t, reader.Next())
				if terminal.err == io.EOF {
					require.NoError(t, reader.Err())
				} else {
					require.ErrorIs(t, reader.Err(), terminal.err)
				}
			})
		}
	}
}

func TestReaderMetadataOnlyCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := metadataFlightStream{ctx: ctx, waiting: make(chan struct{})}
	for i := 0; i < 5; i++ {
		input.payloads = append(input.payloads, &flight.FlightData{AppMetadata: []byte("heartbeat")})
	}
	finished := make(chan error, 1)
	go func() {
		reader, err := flight.NewRecordReader(&input)
		if reader != nil {
			reader.Release()
		}
		finished <- err
	}()
	select {
	case <-input.waiting:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not consume the heartbeats and wait for schema")
	}
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not return after cancellation")
	}
}

func TestReaderRejectsBodyWithoutHeader(t *testing.T) {
	for _, leading := range []int{0, 3} {
		for _, withSchema := range []bool{false, true} {
			t.Run(fmt.Sprintf("leading=%d/schema=%t", leading, withSchema), func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				input := metadataFlightStream{}
				if withSchema {
					record := arrdata.Records["primitives"][0]
					stream := collectingFlightStreamWriter{}
					writer := flight.NewRecordWriter(&stream, ipc.WithSchema(record.Schema()), ipc.WithAllocator(alloc))
					require.NoError(t, writer.Write(record))
					require.NoError(t, writer.Close())
					input.payloads = append(input.payloads, stream.payloads...)
				}
				for i := 0; i < leading; i++ {
					input.payloads = append(input.payloads, &flight.FlightData{AppMetadata: []byte("heartbeat")})
				}
				input.payloads = append(input.payloads, &flight.FlightData{AppMetadata: []byte("heartbeat"), DataBody: []byte{1}})
				reader, err := flight.NewRecordReader(&input, ipc.WithAllocator(alloc))
				if !withSchema {
					require.Nil(t, reader)
					require.ErrorContains(t, err, "body without an IPC header")
					return
				}
				require.NoError(t, err)
				defer reader.Release()
				require.True(t, reader.Next())
				require.False(t, reader.Next())
				require.ErrorContains(t, reader.Err(), "body without an IPC header")
			})
		}
	}
}

func TestReaderMetadataOnlyMessagesWithDictionary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	typ := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.PrimitiveTypes.Int32}
	builder := array.NewDictionaryBuilder(alloc, typ).(*array.Int32DictionaryBuilder)
	require.NoError(t, builder.Append(42))
	require.NoError(t, builder.Append(43))
	builder.AppendNull()
	values := builder.NewDictionaryArray()
	builder.Release()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "number", Type: typ, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	defer record.Release()
	stream := collectingFlightStreamWriter{}
	writer := flight.NewRecordWriter(&stream, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, writer.WriteWithAppMetadata(record, []byte("batch")))
	require.NoError(t, writer.Close())
	input := metadataFlightStream{payloads: []*flight.FlightData{{AppMetadata: []byte("heartbeat")}, {AppMetadata: []byte("heartbeat")}}}
	dictionaries := 0
	for _, payload := range stream.payloads {
		message := ipc.NewMessage(memory.NewBufferBytes(payload.DataHeader), memory.NewBufferBytes(payload.DataBody))
		if message.Type() == ipc.MessageDictionaryBatch {
			dictionaries++
		}
		message.Release()
		input.payloads = append(input.payloads, payload, &flight.FlightData{AppMetadata: []byte("heartbeat")})
	}
	require.Positive(t, dictionaries)
	reader, err := flight.NewRecordReader(&input, ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.True(t, array.RecordEqual(record, reader.RecordBatch()))
	require.Equal(t, []byte("batch"), reader.LatestAppMetadata())
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func TestReaderSchemaValidationAfterMetadata(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	record := arrdata.Records["primitives"][0]
	stream := collectingFlightStreamWriter{}
	writer := flight.NewRecordWriter(&stream, ipc.WithSchema(record.Schema()), ipc.WithAllocator(alloc))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	wrongSchema := arrow.NewSchema([]arrow.Field{{Name: "other", Type: arrow.PrimitiveTypes.Int64}}, nil)
	control := metadataFlightStream{payloads: stream.payloads}
	reader, expectedError := flight.NewRecordReader(&control, ipc.WithSchema(wrongSchema), ipc.WithAllocator(alloc))
	require.Nil(t, reader)
	require.Error(t, expectedError)
	input := metadataFlightStream{payloads: []*flight.FlightData{{AppMetadata: []byte("heartbeat")}, {AppMetadata: []byte("heartbeat")}}}
	input.payloads = append(input.payloads, stream.payloads...)
	reader, err := flight.NewRecordReader(&input, ipc.WithSchema(wrongSchema), ipc.WithAllocator(alloc))
	require.Nil(t, reader)
	require.EqualError(t, err, expectedError.Error())
}

func TestReaderDoesNotIgnoreExplicitIPCNone(t *testing.T) {
	builder := flatbuffers.NewBuilder(0)
	flatbuf.MessageStart(builder)
	flatbuf.MessageAddVersion(builder, flatbuf.MetadataVersionV5)
	builder.Finish(flatbuf.MessageEnd(builder))
	input := metadataFlightStream{payloads: []*flight.FlightData{
		{AppMetadata: []byte("heartbeat")},
		{DataHeader: builder.FinishedBytes(), AppMetadata: []byte("heartbeat")},
	}}
	reader, err := flight.NewRecordReader(&input)
	require.Nil(t, reader)
	require.ErrorContains(t, err, "invalid message type")
}

// metadataFlightStream supplies Flight envelopes without a network dependency.
type metadataFlightStream struct {
	payloads []*flight.FlightData
	next     int
	terminal error
	ctx      context.Context
	waiting  chan struct{}
}

func (f *metadataFlightStream) Recv() (*flight.FlightData, error) {
	if f.next < len(f.payloads) {
		payload := f.payloads[f.next]
		f.next++
		return payload, nil
	}
	if f.ctx != nil {
		close(f.waiting)
		<-f.ctx.Done()
		return nil, f.ctx.Err()
	}
	if f.terminal != nil {
		return nil, f.terminal
	}
	return nil, io.EOF
}
