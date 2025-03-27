package runtime

import (
	"context"

	"github.com/doublemo/nakama-common/api"
)

type Peer interface {
	// InvokeMS invoke microservice
	InvokeMS(ctx context.Context, in *api.AnyRequest) (*api.AnyResponseWriter, error)

	// Send
	SendMS(ctx context.Context, in *api.AnyRequest) error

	//Event
	Event(ctx context.Context, in *api.AnyRequest, names ...string) error
}
