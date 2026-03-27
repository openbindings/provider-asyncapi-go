package asyncapi

import (
	"context"
	"sort"
	"strings"
	"sync"

	openbindings "github.com/openbindings/openbindings-go"
)

// Provider implements BindingExecutor, InterfaceCreator, BindingStreamHandler,
// and ContextInfoProvider for AsyncAPI 3.x specifications.
type Provider struct {
	mu       sync.RWMutex
	docCache map[string]*Document
}

func New() *Provider {
	return &Provider{
		docCache: make(map[string]*Document),
	}
}

// cachedLoadDocument loads an AsyncAPI doc, caching by location within a process.
// When content is provided, the cache is bypassed and updated with the fresh parse.
func (p *Provider) cachedLoadDocument(location string, content any) (*Document, error) {
	if location != "" && content == nil {
		p.mu.RLock()
		if doc, ok := p.docCache[location]; ok {
			p.mu.RUnlock()
			return doc, nil
		}
		p.mu.RUnlock()
	}

	doc, err := loadDocument(location, content)
	if err != nil {
		return nil, err
	}

	if location != "" {
		p.mu.Lock()
		p.docCache[location] = doc
		p.mu.Unlock()
	}
	return doc, nil
}

// GetContextInfo describes the context needed for an AsyncAPI binding.
func (p *Provider) GetContextInfo(_ context.Context, source openbindings.ExecuteSource, _ string) (*openbindings.ContextInfoResult, error) {
	doc, err := p.cachedLoadDocument(source.Location, source.Content)
	if err != nil {
		return nil, err
	}

	key := resolveServerKey(doc)
	if key == "" {
		return nil, nil
	}

	description := doc.Info.Title
	if description == "" {
		description = "AsyncAPI service"
	}

	return &openbindings.ContextInfoResult{
		Key:         key,
		Required:    false,
		Description: description,
	}, nil
}

func resolveServerKey(doc *Document) string {
	serverNames := make([]string, 0, len(doc.Servers))
	for name := range doc.Servers {
		serverNames = append(serverNames, name)
	}
	sort.Strings(serverNames)

	for _, name := range serverNames {
		server := doc.Servers[name]
		proto := strings.ToLower(server.Protocol)
		switch proto {
		case "http", "https", "ws", "wss":
			u := proto + "://" + server.Host
			if server.PathName != "" {
				u += server.PathName
			}
			return strings.TrimRight(u, "/")
		}
	}
	return ""
}

func (p *Provider) Formats() []string {
	return []string{FormatToken}
}

func (p *Provider) ExecuteBinding(ctx context.Context, in *openbindings.BindingExecutionInput) (*openbindings.ExecuteOutput, error) {
	doc, err := p.cachedLoadDocument(in.Source.Location, in.Source.Content)
	if err != nil {
		return nil, err
	}
	return executeBindingWithDoc(ctx, in, doc), nil
}

func (p *Provider) CreateInterface(ctx context.Context, in *openbindings.CreateInput) (*openbindings.Interface, error) {
	if len(in.Sources) == 0 {
		return nil, &openbindings.ExecuteError{Code: "no_sources", Message: "no sources provided"}
	}
	src := in.Sources[0]
	doc, err := p.cachedLoadDocument(src.Location, src.Content)
	if err != nil {
		return nil, err
	}
	return createInterfaceWithDoc(ctx, in, doc)
}

func (p *Provider) SubscribeBinding(ctx context.Context, in *openbindings.BindingExecutionInput) (<-chan openbindings.StreamEvent, error) {
	doc, err := p.cachedLoadDocument(in.Source.Location, in.Source.Content)
	if err != nil {
		return nil, err
	}
	return subscribeBindingWithDoc(ctx, in, doc)
}
