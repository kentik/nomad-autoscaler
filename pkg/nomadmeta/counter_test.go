package nomadmeta

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type counterTestNode struct {
	name   string
	status string
}

func nodeListServer(t *testing.T, nodes []counterTestNode) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stubs := make([]string, 0, len(nodes))

		for _, nd := range nodes {
			stubs = append(stubs, fmt.Sprintf(
				`{"ID":%q,"Name":%q,"Status":%q,"NodePool":"default","SchedulingEligibility":"eligible"}`,
				nd.name, nd.name, nd.status))
		}

		_, _ = w.Write([]byte("[" + strings.Join(stubs, ",") + "]"))
	}))
}

func TestNodeCounter_GetNodeNames_ExcludesDownKeepsDisconnected(t *testing.T) {
	server := nodeListServer(t, []counterTestNode{
		{name: "ready-1", status: api.NodeStatusReady},
		{name: "ready-2", status: api.NodeStatusReady},
		{name: "gone", status: api.NodeStatusDown},
		{name: "partitioned", status: api.NodeStatusDisconnected},
		{name: "starting", status: api.NodeStatusInit},
	})
	defer server.Close()

	cfg := api.DefaultConfig()
	cfg.Address = server.URL
	client, err := api.NewClient(cfg)
	require.NoError(t, err)

	counter := NewCounter(client, hclog.NewNullLogger(), nil, DefaultPageSize)

	names, err := counter.GetNodeNames("", "default")
	require.NoError(t, err)

	// Dropping the disconnected node here would lower the target while its allocs
	// are still holding their slots, so the autoscaler would scale the group down
	// and Nomad would kill those allocs when the node came back.
	assert.ElementsMatch(t, []string{"ready-1", "ready-2", "partitioned", "starting"}, names)
}

func TestNodeCounter_GetNodeNames_AllNodePools(t *testing.T) {
	server := nodeListServer(t, []counterTestNode{
		{name: "ready-1", status: api.NodeStatusReady},
		{name: "gone", status: api.NodeStatusDown},
	})
	defer server.Close()

	cfg := api.DefaultConfig()
	cfg.Address = server.URL
	client, err := api.NewClient(cfg)
	require.NoError(t, err)

	counter := NewCounter(client, hclog.NewNullLogger(), nil, DefaultPageSize)

	names, err := counter.GetNodeNames("", nodePoolAllNodes)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"ready-1"}, names)
}
