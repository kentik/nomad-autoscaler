package node

import (
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
)

func Test_filterUnavailableNodes(t *testing.T) {
	testCases := []struct {
		name     string
		nodes    []*api.NodeListStub
		expected []string
	}{
		{
			name:     "no nodes",
			nodes:    nil,
			expected: []string{},
		},
		{
			name: "down and disconnected are both unavailable",
			nodes: []*api.NodeListStub{
				{ID: "ready", Status: api.NodeStatusReady},
				{ID: "down", Status: api.NodeStatusDown},
				{ID: "disconnected", Status: api.NodeStatusDisconnected},
				{ID: "initializing", Status: api.NodeStatusInit},
			},
			expected: []string{"down", "disconnected"},
		},
		{
			name: "scheduling eligibility does not affect availability",
			nodes: []*api.NodeListStub{
				{ID: "drained", Status: api.NodeStatusReady, SchedulingEligibility: nodeStatusIneligible},
			},
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterUnavailableNodes(tc.nodes)

			assert.Len(t, got, len(tc.expected))
			for _, id := range tc.expected {
				assert.Contains(t, got, id)
			}
		})
	}
}

func Test_filterIneligibleNodes(t *testing.T) {
	nodes := []*api.NodeListStub{
		{ID: "eligible", SchedulingEligibility: "eligible"},
		{ID: "ineligible", SchedulingEligibility: nodeStatusIneligible},
		// A partitioned node stays eligible, which is why disconnects need
		// tracking separately from drains.
		{ID: "disconnected", Status: api.NodeStatusDisconnected, SchedulingEligibility: "eligible"},
	}

	got := filterIneligibleNodes(nodes)

	assert.Len(t, got, 1)
	assert.Contains(t, got, "ineligible")
}

func TestNodeStatusWatcher_updateState(t *testing.T) {
	watcher := &NodeStatusWatcher{
		logger:      hclog.NewNullLogger(),
		initialDone: make(chan bool),
	}

	watcher.updateState([]*api.NodeListStub{
		{ID: "a", Status: api.NodeStatusReady, SchedulingEligibility: "eligible"},
		{ID: "b", Status: api.NodeStatusDisconnected, SchedulingEligibility: "eligible"},
		{ID: "c", Status: api.NodeStatusDown, SchedulingEligibility: nodeStatusIneligible},
	}, nil)

	total, unavailable := watcher.Stats()
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, unavailable)

	assert.True(t, watcher.IsUnavailable("b"))
	assert.True(t, watcher.IsUnavailable("c"))
	assert.False(t, watcher.IsUnavailable("a"))

	assert.True(t, watcher.IsIneligible("c"))
	assert.False(t, watcher.IsIneligible("b"))

	// An API error must not clear the last known good state.
	watcher.updateState(nil, assert.AnError)

	total, unavailable = watcher.Stats()
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, unavailable)
}
