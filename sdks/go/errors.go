package crabka

import (
	"fmt"

	"connectrpc.com/connect"
)

type ErrorKind string

const (
	Transport       ErrorKind = "transport"
	Unauthenticated ErrorKind = "unauthenticated"
	InvalidArgument ErrorKind = "invalid_argument"
	NotFound        ErrorKind = "not_found"
	ServerError     ErrorKind = "server_error"
	Unimplemented   ErrorKind = "unimplemented"
)

type SDKError struct {
	Kind    ErrorKind
	Module  string
	GatedOn string
	Message string
}

func (e *SDKError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.GatedOn != "" {
		return fmt.Sprintf("%s is gated on %s", e.Module, e.GatedOn)
	}
	return string(e.Kind)
}

func gatedError(module string, gatedOn string) *SDKError {
	return &SDKError{Kind: Unimplemented, Module: module, GatedOn: gatedOn}
}

func errorWithMessage(kind ErrorKind, message string) *SDKError {
	return &SDKError{Kind: kind, Message: message}
}

func mapConnectError(err error) *SDKError {
	if err == nil {
		return nil
	}
	message := err.Error()
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated:
		return errorWithMessage(Unauthenticated, message)
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
		return errorWithMessage(InvalidArgument, message)
	case connect.CodeNotFound:
		return errorWithMessage(NotFound, message)
	case connect.CodeUnimplemented:
		return errorWithMessage(Unimplemented, message)
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeCanceled:
		return errorWithMessage(Transport, message)
	default:
		return errorWithMessage(ServerError, message)
	}
}

func mapRecordError(code int32, message string) *SDKError {
	switch code {
	case 3, 9, 11:
		return errorWithMessage(InvalidArgument, message)
	case 5:
		return errorWithMessage(NotFound, message)
	case 16:
		return errorWithMessage(Unauthenticated, message)
	case 12:
		return errorWithMessage(Unimplemented, message)
	case 14:
		return errorWithMessage(Transport, message)
	default:
		return errorWithMessage(ServerError, message)
	}
}
