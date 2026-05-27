package policy

import (
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
)

// Registry stores namespace policies.
type Registry interface {
	Get(namespaceID string) (NamespacePolicy, error)
	GetOrNull(namespaceID string) (*NamespacePolicy, error)
	Put(policy NamespacePolicy) error
	Delete(namespaceID string) (bool, error)
	List() ([]string, error)
}

// InMemoryRegistry is an in-memory policy registry.
type InMemoryRegistry struct {
	mu       sync.Mutex
	policies map[string]NamespacePolicy
}

// NewInMemoryRegistry returns an empty registry.
func NewInMemoryRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{policies: make(map[string]NamespacePolicy)}
}

func (r *InMemoryRegistry) Get(namespaceID string) (NamespacePolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.policies[namespaceID]; ok {
		return p, nil
	}
	return DefaultMutable(namespaceID, nil), nil
}

func (r *InMemoryRegistry) GetOrNull(namespaceID string) (*NamespacePolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.policies[namespaceID]; ok {
		cp := p
		return &cp, nil
	}
	return nil, nil
}

func (r *InMemoryRegistry) Put(policy NamespacePolicy) error {
	result := DefaultValidator.Validate(policy)
	if !result.OK {
		return &ValidationError{Errors: result.Errors}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rev := int64(1)
	if old, ok := r.policies[policy.NamespaceID]; ok {
		rev = old.Revision + 1
	}
	policy.Revision = rev
	r.policies[policy.NamespaceID] = policy
	return nil
}

func (r *InMemoryRegistry) Delete(namespaceID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.policies[namespaceID]
	delete(r.policies, namespaceID)
	return ok, nil
}

func (r *InMemoryRegistry) List() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.policies))
	for id := range r.policies {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// StorageBackedRegistry delegates to an in-memory registry and persists blobs.
type StorageBackedRegistry struct {
	storage  storage.Adapter
	delegate Registry
}

// NewStorageBackedRegistry wraps delegate with blob backup on put.
func NewStorageBackedRegistry(storage storage.Adapter, delegate Registry) *StorageBackedRegistry {
	return &StorageBackedRegistry{storage: storage, delegate: delegate}
}

func (r *StorageBackedRegistry) Get(namespaceID string) (NamespacePolicy, error) {
	return r.delegate.Get(namespaceID)
}

func (r *StorageBackedRegistry) GetOrNull(namespaceID string) (*NamespacePolicy, error) {
	return r.delegate.GetOrNull(namespaceID)
}

func (r *StorageBackedRegistry) Put(policy NamespacePolicy) error {
	if err := r.delegate.Put(policy); err != nil {
		return err
	}
	_, err := r.storage.WriteBlob(encodePolicyStub(policy))
	return err
}

func (r *StorageBackedRegistry) Delete(namespaceID string) (bool, error) {
	return r.delegate.Delete(namespaceID)
}

func (r *StorageBackedRegistry) List() ([]string, error) {
	return r.delegate.List()
}

func encodePolicyStub(p NamespacePolicy) []byte {
	return []byte(p.NamespaceID)
}
