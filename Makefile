gen-go: zipkin.thrift scribe.thrift
	thrift --gen go:package_prefix=github.com/wadey/go-zipkin ./zipkin.thrift
	thrift --gen go:package_prefix=github.com/wadey/go-zipkin ./scribe.thrift
