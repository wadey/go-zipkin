package zipkin

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/wadey/go-zipkin/gen-go/zipkin"
)

type ID int64

type Trace struct {
	Endpoint   *zipkin.Endpoint
	Collectors []SpanCollector
	span       *zipkin.Span
}

func (t *Trace) Child(name string) *Trace {
	return &Trace{
		Endpoint:   t.Endpoint,
		Collectors: t.Collectors,
		span: &zipkin.Span{
			TraceId:  t.span.TraceId,
			Name:     name,
			ParentId: thrift.Int64Ptr(t.span.Id),
			Id:       randomID(),
		},
	}
}

func (t *Trace) SetServiceName(serviceName string) {
	t.Endpoint = &zipkin.Endpoint{
		Ipv4:        t.Endpoint.Ipv4,
		Port:        t.Endpoint.Port,
		ServiceName: serviceName,
	}
}

func (t *Trace) Record(annotation *zipkin.Annotation) {
	// TODO mutex?
	if annotation.Host == nil {
		annotation.Host = t.Endpoint
	}
	t.span.Annotations = append(t.span.Annotations, annotation)

	// TODO some way to catch dangling traces
	if annotation.Value == zipkin.CLIENT_RECV || annotation.Value == zipkin.SERVER_SEND {
		t.Submit()
	}
}

func (t *Trace) RecordBinary(annotation *zipkin.BinaryAnnotation) {
	if annotation.Host == nil {
		annotation.Host = t.Endpoint
	}
	t.span.BinaryAnnotations = append(t.span.BinaryAnnotations, annotation)
}

func (t *Trace) Submit() {
	clone := &zipkin.Span{
		TraceId:           t.span.TraceId,
		Name:              t.span.Name,
		Id:                t.span.Id,
		ParentId:          t.span.ParentId,
		Annotations:       t.span.Annotations,
		BinaryAnnotations: t.span.BinaryAnnotations,
		Debug:             t.span.Debug,
	}
	for _, c := range t.Collectors {
		c.Collect(clone)
	}
	t.span.Annotations = nil
	t.span.BinaryAnnotations = nil
}

func (t *Trace) HTTPHeader() http.Header {
	h := http.Header{}
	h.Set("X-B3-TraceId", ID(t.span.TraceId).String())
	h.Set("X-B3-SpanId", ID(t.span.Id).String())
	if t.span.IsSetParentId() {
		h.Set("X-B3-ParentSpanId", ID(t.span.GetParentId()).String())
	}
	return h
}

func (t *Trace) TraceID() ID {
	return ID(t.span.TraceId)
}
func (t *Trace) SpanID() ID {
	return ID(t.span.Id)
}
func (t *Trace) ParentSpanID() *ID {
	if t.span.IsSetParentId() {
		id := ID(*t.span.ParentId)
		return &id
	}
	return nil
}

func NewTrace(traceName string, collectors []SpanCollector) *Trace {
	return NewTraceForIDs(traceName, randomID(), randomID(), nil, collectors)
}

func NewTraceForIDs(traceName string, traceID, spanID int64, parentSpanID *int64, collectors []SpanCollector) *Trace {
	span := &zipkin.Span{
		Name:     traceName,
		TraceId:  traceID,
		Id:       spanID,
		ParentId: parentSpanID,
	}
	return &Trace{
		Collectors: collectors,
		span:       span,
	}
}

func NewTraceForHTTPHeader(traceName string, h http.Header, collectors []SpanCollector) *Trace {
	var traceID, spanID int64
	var parentSpanID *int64

	if s := maybeReadID(h.Get("X-B3-TraceId")); s != nil {
		traceID = int64(*s)
	} else {
		traceID = randomID()
	}
	if s := maybeReadID(h.Get("X-B3-SpanId")); s != nil {
		spanID = int64(*s)
	} else {
		spanID = randomID()
	}
	if s := maybeReadID(h.Get("X-B3-ParentSpanId")); s != nil {
		parentSpanID = thrift.Int64Ptr(int64(*s))
	}

	return NewTraceForIDs(traceName, traceID, spanID, parentSpanID, collectors)
}

func NewTimestampAnnotation(value string, t time.Time) *zipkin.Annotation {
	if t.IsZero() {
		t = time.Now()
	}

	return &zipkin.Annotation{
		Timestamp: t.UnixNano() / 1e3,
		Value:     value
	}
}

func ClientSendAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.CLIENT_SEND, t)
}

func ClientRecvAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.CLIENT_RECV, t)
}

func ServerSendAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.SERVER_SEND, t)
}

func ServerRecvAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.SERVER_RECV, t)
}

func NewStringAnnotation(key string, value string) *zipkin.BinaryAnnotation {
	return &zipkin.BinaryAnnotation{
		Key:            key,
		Value:          []byte(value),
		AnnotationType: zipkin.AnnotationType_STRING,
	}
}

type SpanCollector interface {
	Collect(span *zipkin.Span)
}

type DebugCollector struct{}

func (*DebugCollector) Collect(span *zipkin.Span) {
	log.Printf("[%s %s %s %s] %s %s",
		ID(span.TraceId), (*ID)(span.ParentId), ID(span.Id), span.Name,
		span.Annotations, span.BinaryAnnotations)
}

func (id ID) String() string {
	b := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(b, binary.BigEndian, id)
	return hex.EncodeToString(b.Bytes())
}

func maybeReadID(s string) *ID {
	b, err := hex.DecodeString(s)
	if err == nil && len(b) == 8 {
		var id ID
		binary.Read(bytes.NewReader(b), binary.BigEndian, &id)
		return &id
	}
	return nil
}

// TODO use a 64-bit RNG like xorshift or make it customizable?
func randomID() int64 {
	// Mask off to 2^53 because lol javascript
	// https://github.com/twitter/zipkin/issues/199
	return rand.Int63() & 0x001fffffffffffff
}
