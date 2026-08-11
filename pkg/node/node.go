package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk/helper/blocking"
	"github.com/hashicorp/nomad/api"
)

//go:generate mockgen -destination=../../mocks/pkg/node/node.go . Status

type Status interface {
	IsIneligible(nodeID string) bool
	IsUnavailable(nodeID string) bool
	Stats() (total int, unavailable int)
}

type NodeStatusWatcher struct {
	client *api.Client
	logger hclog.Logger

	initialDone chan bool
	initialized bool

	// isRunning details whether the loop within start() is currently running
	// or not.
	isRunning bool

	// lastUpdated is the UnixNano UTC timestamp of the last update to the
	// state. This helps with garbage collection.
	lastUpdated int64

	ineligibleNodes map[string]struct{}

	// unavailableNodes holds nodes that cannot host allocations, whether the
	// client is down or only partitioned away from the servers.
	unavailableNodes map[string]struct{}
	totalNodes       int

	mw sync.RWMutex // protects the node maps, totalNodes and isRunning
}

var (
	// nodeStatusWatcherInitTimeout is the time limit the plugin must wait
	// before considering the operation a failure.
	nodeStatusWatcherInitTimeout = 30 * time.Second
)

func NewNodeStatusWatcher(client *api.Client, logger hclog.Logger) (*NodeStatusWatcher, error) {
	watcher := &NodeStatusWatcher{
		client:      client,
		logger:      logger,
		initialDone: make(chan bool),
	}

	go watcher.loop()

	// Wait for initial status data to be loaded.
	// Set a timeout to make sure callers are not blocked indefinitely.
	select {
	case <-watcher.initialDone:
	case <-time.After(nodeStatusWatcherInitTimeout):
		watcher.setStopState()
		return nil, fmt.Errorf("timeout while waiting for job scale status handler")
	}

	return watcher, nil
}

func (n *NodeStatusWatcher) loop() {
	n.logger.Info("starting node status watcher")

	n.mw.Lock()
	n.isRunning = true
	n.mw.Unlock()

	q := &api.QueryOptions{
		WaitTime:  5 * time.Minute,
		WaitIndex: 1,
	}

	for {
		nodes, meta, err := n.client.Nodes().List(q)

		n.updateState(nodes, err)

		if err != nil {
			n.logger.Debug("failed to list nodes, retrying", "error", err)

			// Reset query WaitIndex to zero so we can get the job status
			// immediately in the next request instead of blocking and having
			// to wait for a timeout.
			q.WaitIndex = 0

			// If the error was anything other than the job not being found,
			// try again.
			time.Sleep(10 * time.Second)
			continue
		}

		// Read handler state into local variables so we don't starve the lock.
		n.mw.RLock()
		isRunning := n.isRunning
		ineligibleStatus := n.ineligibleNodes
		n.mw.RUnlock()

		// Stop loop if handler is not running anymore.
		if !isRunning {
			return
		}

		// If the index has not changed, the query returned because the timeout
		// was reached, therefore start the next query loop.
		// The index could also be the same when a reconnect happens, in which
		// case the handler state needs to be updated regardless of the index.
		if ineligibleStatus != nil && !blocking.IndexHasChanged(meta.LastIndex, q.WaitIndex) {
			continue
		}

		// Modify the wait index on the QueryOptions so the blocking query
		// is using the latest index value.
		q.WaitIndex = meta.LastIndex
	}
}

func (n *NodeStatusWatcher) IsIneligible(nodeID string) bool {
	n.mw.RLock()
	defer n.mw.RUnlock()

	_, exists := n.ineligibleNodes[nodeID]
	return exists
}

func (n *NodeStatusWatcher) IsUnavailable(nodeID string) bool {
	n.mw.RLock()
	defer n.mw.RUnlock()

	_, exists := n.unavailableNodes[nodeID]
	return exists
}

func (n *NodeStatusWatcher) Stats() (int, int) {
	n.mw.RLock()
	defer n.mw.RUnlock()

	return n.totalNodes, len(n.unavailableNodes)
}

func (n *NodeStatusWatcher) SetClient(client *api.Client) {
	n.mw.Lock()
	defer n.mw.Unlock()
	n.client = client
}

func (n *NodeStatusWatcher) updateState(nodes []*api.NodeListStub, err error) {
	n.mw.Lock()
	defer n.mw.Unlock()

	if err != nil { // do not update the list if api call returned an error
		return
	}

	// Mark the handler as initialized and notify initialDone channel.
	if !n.initialized {
		n.initialized = true

		// Close channel so we don't block waiting for readers.
		// n.initialized should only be set once to avoid closing this twice.
		close(n.initialDone)
	}

	before := len(n.ineligibleNodes)
	beforeUnavailable := len(n.unavailableNodes)

	n.ineligibleNodes = filterIneligibleNodes(nodes)
	n.unavailableNodes = filterUnavailableNodes(nodes)
	n.totalNodes = len(nodes)
	n.lastUpdated = time.Now().UTC().UnixNano()

	after := len(n.ineligibleNodes)
	afterUnavailable := len(n.unavailableNodes)

	if before != after {
		n.logger.Info("updating ineligible status", "ineligible", after)
	}

	if beforeUnavailable != afterUnavailable {
		n.logger.Info("updating unavailable status",
			"unavailable", afterUnavailable, "total", n.totalNodes)
	}
}

func (n *NodeStatusWatcher) setStopState() {
	n.mw.Lock()
	defer n.mw.Unlock()

	n.isRunning = false
	n.ineligibleNodes = nil
	n.unavailableNodes = nil
	n.totalNodes = 0
	n.lastUpdated = time.Now().UTC().UnixNano()
}

const (
	nodeStatusIneligible = "ineligible"
)

func filterIneligibleNodes(nodes []*api.NodeListStub) map[string]struct{} {
	ineligible := make(map[string]struct{})

	for _, node := range nodes {
		if node.SchedulingEligibility == nodeStatusIneligible {
			ineligible[node.ID] = struct{}{}
		}
	}

	return ineligible
}

// filterUnavailableNodes returns nodes that cannot host allocations. A
// partitioned client reports "disconnected" and only becomes "down" once its
// disconnect.lost_after window expires, so both must be treated as lost
// capacity to avoid a cliff-edge count drop at expiry.
func filterUnavailableNodes(nodes []*api.NodeListStub) map[string]struct{} {
	unavailable := make(map[string]struct{})

	for _, node := range nodes {
		if node.Status == api.NodeStatusDown || node.Status == api.NodeStatusDisconnected {
			unavailable[node.ID] = struct{}{}
		}
	}

	return unavailable
}
