package zipkin

import (
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/wadey/go-zipkin/gen-go/zipkin"
)

var (
	traces = map[*url.URL]*Trace{}
	mutex  sync.Mutex
)

type handler struct {
	next    http.Handler
	options *Options
}

type Options struct {
	Enabled     bool
	Sample      float64
	Collectors  []SpanCollector
	IP          net.IP
	ipNum       int32
	Port        int16
	ServiceName string
}

func Handler(next http.Handler, options *Options) http.Handler {
	if options == nil || !options.Enabled {
		return next
	}
	options.ipNum = ipv4ToNumber(options.IP)
	return &handler{
		next:    next,
		options: options,
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.options.Enabled && r.Header.Get("X-B3-TraceId") != "" || rand.Float64() < h.options.Sample {
		trace := NewTraceForHTTPHeader(r.Method, r.Header, h.options.Collectors)
		trace.Endpoint = &zipkin.Endpoint{
			Ipv4:        h.options.ipNum,
			Port:        h.options.Port,
			ServiceName: h.options.ServiceName,
		}
		setCurrentTrace(r, trace)
		defer delCurrentTrace(r)
		trace.Record(ServerRecvAnnotation(time.Now()))
		trace.RecordBinary(NewStringAnnotation("http.uri", r.URL.RequestURI()))
		w = &zipkinResponseWriter{trace: trace, writer: w}
	}
	h.next.ServeHTTP(w, r)
}

type zipkinResponseWriter struct {
	trace  *Trace
	writer http.ResponseWriter
}

func (w *zipkinResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *zipkinResponseWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

func (w *zipkinResponseWriter) WriteHeader(i int) {
	w.trace.RecordBinary(NewStringAnnotation("http.response.code", strconv.FormatInt(int64(i), 10)))
	w.trace.Record(ServerSendAnnotation(time.Now()))
	w.writer.WriteHeader(i)
}

func setCurrentTrace(r *http.Request, t *Trace) {
	mutex.Lock()
	defer mutex.Unlock()
	traces[r.URL] = t
}

func delCurrentTrace(r *http.Request) {
	mutex.Lock()
	defer mutex.Unlock()
	delete(traces, r.URL)
}

func CurrentTrace(r *http.Request) *Trace {
	mutex.Lock()
	defer mutex.Unlock()
	return traces[r.URL]
}

func CurrentTraceForURL(u *url.URL) *Trace {
	mutex.Lock()
	defer mutex.Unlock()
	return traces[u]
}

func ipv4ToNumber(ip net.IP) (sum int32) {
	for i := 12; i < 16; i++ {
		sum = (sum << 8) + int32(ip[i])
	}
	return
}
