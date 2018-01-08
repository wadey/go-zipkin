gen-go:
	thrift --gen go:package_prefix=github.com/wadey/go-zipkin/gen-go/ ./zipkin.thrift
	thrift --gen go:package_prefix=github.com/wadey/go-zipkin/gen-go/ ./scribe.thrift
	sed -E -i.orig 's#git.apache.org/thrift.git/lib/go/thrift#github.com/betable/go-thrift/thrift#' gen-go/scribe/*.go gen-go/scribe/scribe-remote/*.go gen-go/zipkin/*.go
	rm gen-go/scribe/*.orig gen-go/scribe/scribe-remote/*.orig gen-go/zipkin/*.orig

.PHONY: gen-go
