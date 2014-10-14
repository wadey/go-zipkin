package zipkin

import (
	"encoding/base64"
	"log"
	"net"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/wadey/go-zipkin/gen-go/scribe"
	"github.com/wadey/go-zipkin/gen-go/zipkin"
)

const (
	buffer  = 128
	timeout = 10 * time.Second
)

type ScribeCollector struct {
	addr     string
	category string
	spanChan chan *zipkin.Span
}

func NewScribeCollector(addr string, category string) *ScribeCollector {
	c := &ScribeCollector{
		addr:     addr,
		spanChan: make(chan *zipkin.Span, buffer),
	}
	go c.HandleConnection()
	return c
}

func (c *ScribeCollector) Collect(span *zipkin.Span) {
	select {
	case c.spanChan <- span:
	default:
	}
}

func (c *ScribeCollector) newConnection() (scribe.Scribe, error) {
	addr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	socket := thrift.NewTSocketFromAddrTimeout(addr, timeout)
	transport := thrift.NewTFramedTransport(socket)
	if err := transport.Open(); err != nil {
		return nil, err
	}
	proto := thrift.NewTBinaryProtocolTransport(transport)
	s := scribe.NewScribeClientProtocol(transport, proto, proto)
	return s, nil
}

func (c *ScribeCollector) HandleConnection() {
	var scribeClient scribe.Scribe
	var err error

	log.Printf("starting zipkin collector to: %v", c.addr)
	defer log.Println("quiting zipkin collector")
	for {
		select {
		case s, ok := <-c.spanChan:
			if !ok {
				return
			}
			if scribeClient == nil {
				scribeClient, err = c.newConnection()
				if err != nil {
					log.Printf("zipkin: error opening scribe connection to %v: %v", c.addr, err)
					continue
				}
			}

			// TODO batch
			b, err := spanToBytes(s)
			if err != nil {
				log.Printf("zipkin: failed to serialize span: %v", err)
				continue
			}
			message := base64.StdEncoding.EncodeToString(b)

			logEntry := &scribe.LogEntry{
				Category: c.category,
				Message:  message,
			}

			_, err = scribeClient.Log([]*scribe.LogEntry{logEntry})
			if err != nil {
				log.Printf("zipkin: failed to send span to %v: %v", c.addr, err)
				scribeClient = nil
			}
		}
	}
}

func spanToBytes(span *zipkin.Span) ([]byte, error) {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	err := span.Write(p)
	if err != nil {
		return nil, err
	}
	return t.Buffer.Bytes(), nil
}
