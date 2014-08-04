package zipkin

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"log"
	"math/rand"
	"net/http"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
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
	for _, c := range t.Collectors {
		c.Collect(t.span)
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

func NewTraceForHTTPHeader(traceName string, h http.Header, collectors []SpanCollector) *Trace {
	span := &zipkin.Span{
		Name: traceName,
	}
	if s := maybeReadID(h.Get("X-B3-TraceId")); s != nil {
		span.TraceId = int64(*s)
	} else {
		span.TraceId = randomID()
	}
	if s := maybeReadID(h.Get("X-B3-SpanId")); s != nil {
		span.Id = int64(*s)
	} else {
		span.Id = randomID()
	}
	if s := maybeReadID(h.Get("X-B3-ParentSpanId")); s != nil {
		span.ParentId = thrift.Int64Ptr(int64(*s))
	}
	return &Trace{
		Collectors: collectors,
		span:       span,
	}
}

func NewTimestampAnnotation(value string, t time.Time, d time.Duration) *zipkin.Annotation {
	if t.IsZero() {
		t = time.Now()
	}
	return &zipkin.Annotation{
		Timestamp: t.UnixNano() / 1e3,
		Value:     value,
		Duration:  thrift.Int32Ptr(int32(d / time.Microsecond)),
	}
}

func ClientSendAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.CLIENT_SEND, t, 0)
}

func ClientRecvAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.CLIENT_RECV, t, 0)
}

func ServerSendAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.SERVER_SEND, t, 0)
}

func ServerRecvAnnotation(t time.Time) *zipkin.Annotation {
	return NewTimestampAnnotation(zipkin.SERVER_RECV, t, 0)
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
	return rand.Int63()
}
