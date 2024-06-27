// Copyright 2024 The Bombus Authors
//
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package kit

import (
	"sync"

	"go.uber.org/atomic"
)

type ConnectorRegistry struct {
	connectors     *MapOf[string, Connector]
	connectorRoles map[string]map[string]bool
	connectorCount *atomic.Int32
	sync.RWMutex
}

func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{
		connectors:     &MapOf[string, Connector]{},
		connectorRoles: make(map[string]map[string]bool),
		connectorCount: atomic.NewInt32(0),
	}
}

func (r *ConnectorRegistry) Count() int {
	return int(r.connectorCount.Load())
}

func (r *ConnectorRegistry) Get(id string) (Connector, bool) {
	return r.connectors.Load(id)
}

func (r *ConnectorRegistry) Add(conn Connector) {
	r.connectors.Store(conn.ID(), conn)
	r.Lock()
	m, ok := r.connectorRoles[conn.Role()]
	if !ok {
		r.connectorRoles[conn.Role()] = map[string]bool{conn.ID(): true}
	} else {
		if m[conn.ID()] {
			r.Unlock()
			return
		}
		m[conn.ID()] = true
	}
	r.Unlock()
	r.connectorCount.Inc()
}

func (r *ConnectorRegistry) Remove(id string) {
	conn, ok := r.connectors.Load(id)
	if !ok {
		return
	}

	r.connectors.Delete(id)
	r.connectorCount.Dec()
	r.Lock()
	m, ok := r.connectorRoles[conn.Role()]
	if !ok {
		r.Unlock()
		return
	}
	delete(m, id)
	if size := len(r.connectorRoles[conn.Role()]); size < 1 {
		delete(r.connectorRoles, conn.Role())
	}
	r.Unlock()
}

func (r *ConnectorRegistry) Range(fn func(Connector) bool) {
	r.connectors.Range(func(id string, conn Connector) bool {
		return fn(conn)
	})
}

func (r *ConnectorRegistry) RangeRole(role string, fn func(Connector) bool) {
	r.RLock()
	m, ok := r.connectorRoles[role]
	if !ok {
		r.RUnlock()
		return
	}

	for id := range m {
		r.RUnlock()
		conn, ok := r.connectors.Load(id)
		if ok {
			fn(conn)
		}
		r.RLock()
	}
	r.RUnlock()
}
