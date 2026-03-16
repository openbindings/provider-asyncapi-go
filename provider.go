package asyncapi

import (
	"context"

	openbindings "github.com/openbindings/openbindings-go"
)

// Provider implements BindingExecutor, InterfaceCreator, and BindingStreamHandler
// for AsyncAPI 3.x specifications.
type Provider struct{}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Formats() []string {
	return []string{FormatToken}
}

func (p *Provider) ExecuteBinding(ctx context.Context, in *openbindings.BindingExecutionInput) (*openbindings.ExecuteOutput, error) {
	return executeBinding(ctx, in), nil
}

func (p *Provider) CreateInterface(ctx context.Context, in *openbindings.CreateInput) (*openbindings.Interface, error) {
	return createInterface(ctx, in)
}

func (p *Provider) SubscribeBinding(ctx context.Context, in *openbindings.BindingExecutionInput) (<-chan openbindings.StreamEvent, error) {
	return subscribeBinding(ctx, in)
}
