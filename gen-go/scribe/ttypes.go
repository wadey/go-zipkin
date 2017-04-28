// Autogenerated by Thrift Compiler (1.0.0-dev)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package scribe

import (
	"bytes"
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = bytes.Equal

var GoUnusedProtection__ int

type ResultCode int64

const (
	ResultCode_OK        ResultCode = 0
	ResultCode_TRY_LATER ResultCode = 1
)

func (p ResultCode) String() string {
	switch p {
	case ResultCode_OK:
		return "ResultCode_OK"
	case ResultCode_TRY_LATER:
		return "ResultCode_TRY_LATER"
	}
	return "<UNSET>"
}

func ResultCodeFromString(s string) (ResultCode, error) {
	switch s {
	case "ResultCode_OK":
		return ResultCode_OK, nil
	case "ResultCode_TRY_LATER":
		return ResultCode_TRY_LATER, nil
	}
	return ResultCode(0), fmt.Errorf("not a valid ResultCode string")
}

func ResultCodePtr(v ResultCode) *ResultCode { return &v }

type LogEntry struct {
	Category string `thrift:"category,1" json:"category"`
	Message  string `thrift:"message,2" json:"message"`
}

func NewLogEntry() *LogEntry {
	return &LogEntry{}
}

func (p *LogEntry) GetCategory() string {
	return p.Category
}

func (p *LogEntry) GetMessage() string {
	return p.Message
}
func (p *LogEntry) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return fmt.Errorf("%T read error: %s", p, err)
	}
	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return fmt.Errorf("%T field %d read error: %s", p, fieldId, err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if err := p.ReadField1(iprot); err != nil {
				return err
			}
		case 2:
			if err := p.ReadField2(iprot); err != nil {
				return err
			}
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return fmt.Errorf("%T read struct end error: %s", p, err)
	}
	return nil
}

func (p *LogEntry) ReadField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return fmt.Errorf("error reading field 1: %s", err)
	} else {
		p.Category = v
	}
	return nil
}

func (p *LogEntry) ReadField2(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return fmt.Errorf("error reading field 2: %s", err)
	} else {
		p.Message = v
	}
	return nil
}

func (p *LogEntry) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("LogEntry"); err != nil {
		return fmt.Errorf("%T write struct begin error: %s", p, err)
	}
	if err := p.writeField1(oprot); err != nil {
		return err
	}
	if err := p.writeField2(oprot); err != nil {
		return err
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return fmt.Errorf("write field stop error: %s", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return fmt.Errorf("write struct stop error: %s", err)
	}
	return nil
}

func (p *LogEntry) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("category", thrift.STRING, 1); err != nil {
		return fmt.Errorf("%T write field begin error 1:category: %s", p, err)
	}
	if err := oprot.WriteString(string(p.Category)); err != nil {
		return fmt.Errorf("%T.category (1) field write error: %s", p, err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return fmt.Errorf("%T write field end error 1:category: %s", p, err)
	}
	return err
}

func (p *LogEntry) writeField2(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("message", thrift.STRING, 2); err != nil {
		return fmt.Errorf("%T write field begin error 2:message: %s", p, err)
	}
	if err := oprot.WriteString(string(p.Message)); err != nil {
		return fmt.Errorf("%T.message (2) field write error: %s", p, err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return fmt.Errorf("%T write field end error 2:message: %s", p, err)
	}
	return err
}

func (p *LogEntry) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("LogEntry(%+v)", *p)
}
