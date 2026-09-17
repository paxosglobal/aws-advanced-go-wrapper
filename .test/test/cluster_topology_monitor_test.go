/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package test

import (
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const staleTopologyClusterId = "stale-topology-cluster"

// noWriterTopologyStrategy stands in for a demoted secondary cluster: every
// instance answers, none of them is the writer.
type noWriterTopologyStrategy struct {
	hosts []*host_info_util.HostInfo
}

func (s *noWriterTopologyStrategy) QueryForTopology(driver.Conn) ([]*host_info_util.HostInfo, error) {
	return s.hosts, nil
}

func (s *noWriterTopologyStrategy) GetInstanceTemplate(string, driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (s *noWriterTopologyStrategy) IsWriterInstance(driver.Conn) (bool, error) {
	return false, nil
}

func (s *noWriterTopologyStrategy) GetInstanceId(driver.Conn) (string, string) {
	return "", ""
}

func (s *noWriterTopologyStrategy) CreateHost(
	_, _ string, _ bool, _ int, _ time.Time, initialHost, _ *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return initialHost
}

func staleHost(t *testing.T, host string) *host_info_util.HostInfo {
	t.Helper()
	info, err := host_info_util.NewHostInfoBuilder().
		SetHost(host).
		SetPort(5432).
		SetHostId(host).
		SetRole(host_info_util.READER).
		SetAvailability(host_info_util.AVAILABLE).
		Build()
	require.NoError(t, err)
	return info
}

// TestClusterTopologyMonitorRechecksInitialHostWhenStalled covers the stale,
// self-renewing topology cache: the cached hosts stay reachable but never yield
// a writer, so the empty-cache fallback to the initial host never fires on its
// own and the monitor can never discover the promoted cluster. The monitor must
// re-consult the initial host anyway once it has been stalled long enough.
func TestClusterTopologyMonitorRechecksInitialHostWhenStalled(t *testing.T) {
	originalInterval := driver_infrastructure.InitialHostRecheckIntervalNano
	driver_infrastructure.InitialHostRecheckIntervalNano = 100 * time.Millisecond
	defer func() { driver_infrastructure.InitialHostRecheckIntervalNano = originalInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHostInfo := staleHost(t, "cluster.global-abc.global.rds.amazonaws.com")
	staleHosts := []*host_info_util.HostInfo{
		staleHost(t, "old-region-instance-0.abc.us-east-1.rds.amazonaws.com"),
		staleHost(t, "old-region-instance-1.abc.us-east-1.rds.amazonaws.com"),
	}

	var initialHostConnects atomic.Int32

	mockDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsNetworkError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().IsLoginError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER).AnyTimes()
	mockPluginService.EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		DoAndReturn(func(hostInfo *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
			if hostInfo.GetHost() == initialHostInfo.GetHost() {
				initialHostConnects.Add(1)
			}
			// A fresh conn per call: the routines close them independently.
			return &MockConn{}, nil
		}).
		AnyTimes()

	publisher := services.NewEventPublisher()
	storage := services.NewExpiringStorage(time.Minute, publisher)
	container := &services.FullServicesContainer{Storage: storage, Events: publisher}

	// Seed the cache with a non-empty, stale topology, exactly as a switchover
	// leaves it. Without the recheck the monitor never looks past these hosts.
	driver_infrastructure.TopologyStorageType.Register(storage)
	driver_infrastructure.TopologyStorageType.Set(
		storage, staleTopologyClusterId, driver_infrastructure.NewTopology(staleHosts))

	// Guard against the test passing for the wrong reason: if the cache were
	// empty the monitor would consult the initial host through the pre-existing
	// fallback, proving nothing about the recheck.
	seeded, found := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
	require.True(t, found)
	require.Len(t, seeded.GetHosts(), len(staleHosts))

	monitor := driver_infrastructure.NewClusterTopologyMonitorImpl(
		container,
		staleTopologyClusterId,
		10*time.Millisecond,
		10*time.Millisecond,
		time.Minute,
		utils.NewRWMap[string, string](),
		initialHostInfo,
		initialHostInfo,
		mockPluginService,
		&noWriterTopologyStrategy{hosts: staleHosts},
	)

	monitor.Start()
	defer monitor.Stop()

	assert.Eventually(t,
		func() bool { return initialHostConnects.Load() > 0 },
		5*time.Second, 20*time.Millisecond,
		"monitor never re-consulted the initial host while stalled with a stale topology cache")
}

// TestClusterTopologyMonitorDoesNotRecheckImmediately guards the other side: a
// brief panic mode, which is what an ordinary failover looks like, must not pay
// for an extra connection to the initial host.
func TestClusterTopologyMonitorDoesNotRecheckImmediately(t *testing.T) {
	originalInterval := driver_infrastructure.InitialHostRecheckIntervalNano
	driver_infrastructure.InitialHostRecheckIntervalNano = time.Hour
	defer func() { driver_infrastructure.InitialHostRecheckIntervalNano = originalInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHostInfo := staleHost(t, "cluster.global-def.global.rds.amazonaws.com")
	staleHosts := []*host_info_util.HostInfo{
		staleHost(t, "old-region-instance-0.def.us-east-1.rds.amazonaws.com"),
	}

	var initialHostConnects atomic.Int32

	mockDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsNetworkError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().IsLoginError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER).AnyTimes()
	mockPluginService.EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		DoAndReturn(func(hostInfo *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
			if hostInfo.GetHost() == initialHostInfo.GetHost() {
				initialHostConnects.Add(1)
			}
			// A fresh conn per call: the routines close them independently.
			return &MockConn{}, nil
		}).
		AnyTimes()

	publisher := services.NewEventPublisher()
	storage := services.NewExpiringStorage(time.Minute, publisher)
	container := &services.FullServicesContainer{Storage: storage, Events: publisher}

	driver_infrastructure.TopologyStorageType.Register(storage)
	driver_infrastructure.TopologyStorageType.Set(
		storage, staleTopologyClusterId, driver_infrastructure.NewTopology(staleHosts))

	// Guard against the test passing for the wrong reason: if the cache were
	// empty the monitor would consult the initial host through the pre-existing
	// fallback, proving nothing about the recheck.
	seeded, found := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
	require.True(t, found)
	require.Len(t, seeded.GetHosts(), len(staleHosts))

	monitor := driver_infrastructure.NewClusterTopologyMonitorImpl(
		container,
		staleTopologyClusterId,
		10*time.Millisecond,
		10*time.Millisecond,
		time.Minute,
		utils.NewRWMap[string, string](),
		initialHostInfo,
		initialHostInfo,
		mockPluginService,
		&noWriterTopologyStrategy{hosts: staleHosts},
	)

	monitor.Start()
	defer monitor.Stop()

	time.Sleep(300 * time.Millisecond)

	assert.Zero(t, initialHostConnects.Load(),
		"monitor re-consulted the initial host before the recheck interval elapsed")
}

// identifiedConn tags a connection with the host it was opened against, so a
// strategy can answer differently for the initial host than for cached hosts.
type identifiedConn struct {
	driver.Conn
	host string
}

// promotedClusterStrategy models the state right after a switchover: the cached
// hosts belong to the demoted region and never report a writer, while the
// initial host now resolves to the promoted cluster and does.
type promotedClusterStrategy struct {
	initialHost string
	staleHosts  []*host_info_util.HostInfo
	newHosts    []*host_info_util.HostInfo
}

func (s *promotedClusterStrategy) isInitialHost(conn driver.Conn) bool {
	identified, ok := conn.(*identifiedConn)
	return ok && identified.host == s.initialHost
}

func (s *promotedClusterStrategy) QueryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	if s.isInitialHost(conn) {
		return s.newHosts, nil
	}
	return s.staleHosts, nil
}

func (s *promotedClusterStrategy) GetInstanceTemplate(string, driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (s *promotedClusterStrategy) IsWriterInstance(conn driver.Conn) (bool, error) {
	return s.isInitialHost(conn), nil
}

func (s *promotedClusterStrategy) GetInstanceId(conn driver.Conn) (string, string) {
	if s.isInitialHost(conn) {
		return s.newHosts[0].HostId, s.newHosts[0].GetHost()
	}
	return "", ""
}

func (s *promotedClusterStrategy) CreateHost(
	_, _ string, _ bool, _ int, _ time.Time, initialHost, _ *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return initialHost
}

// TestClusterTopologyMonitorRecoversPromotedClusterWhenStalled is the end of the
// story the previous test starts. It is not enough that the monitor re-consults
// the initial host: that connection must verify a writer, leave panic mode, and
// replace the stale cache with the promoted cluster's topology. Without the
// recheck the monitor never reaches the promoted cluster at all.
func TestClusterTopologyMonitorRecoversPromotedClusterWhenStalled(t *testing.T) {
	originalInterval := driver_infrastructure.InitialHostRecheckIntervalNano
	driver_infrastructure.InitialHostRecheckIntervalNano = 100 * time.Millisecond
	defer func() { driver_infrastructure.InitialHostRecheckIntervalNano = originalInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHostInfo := staleHost(t, "cluster.global-ghi.global.rds.amazonaws.com")
	staleHosts := []*host_info_util.HostInfo{
		staleHost(t, "old-region-instance-0.ghi.us-east-1.rds.amazonaws.com"),
		staleHost(t, "old-region-instance-1.ghi.us-east-1.rds.amazonaws.com"),
	}
	promotedHost := staleHost(t, "new-region-instance-0.jkl.us-west-2.rds.amazonaws.com")

	mockDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsNetworkError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().IsLoginError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER).AnyTimes()
	mockPluginService.EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		DoAndReturn(func(hostInfo *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
			return &identifiedConn{Conn: &MockConn{}, host: hostInfo.GetHost()}, nil
		}).
		AnyTimes()

	publisher := services.NewEventPublisher()
	storage := services.NewExpiringStorage(time.Minute, publisher)
	container := &services.FullServicesContainer{Storage: storage, Events: publisher}

	driver_infrastructure.TopologyStorageType.Register(storage)
	driver_infrastructure.TopologyStorageType.Set(
		storage, staleTopologyClusterId, driver_infrastructure.NewTopology(staleHosts))

	seeded, found := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
	require.True(t, found)
	require.Len(t, seeded.GetHosts(), len(staleHosts))

	monitor := driver_infrastructure.NewClusterTopologyMonitorImpl(
		container,
		staleTopologyClusterId,
		10*time.Millisecond,
		10*time.Millisecond,
		time.Minute,
		utils.NewRWMap[string, string](),
		initialHostInfo,
		initialHostInfo,
		mockPluginService,
		&promotedClusterStrategy{
			initialHost: initialHostInfo.GetHost(),
			staleHosts:  staleHosts,
			newHosts:    []*host_info_util.HostInfo{promotedHost},
		},
	)

	monitor.Start()
	defer monitor.Stop()

	assert.Eventually(t, func() bool {
		topology, ok := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
		if !ok {
			return false
		}
		for _, h := range topology.GetHosts() {
			if h.GetHost() == promotedHost.GetHost() {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond,
		"cache still holds the demoted region's topology; the monitor never picked up the promoted cluster")

	// Leaving panic mode is what makes waitForTopologyUpdate stop timing out,
	// which is the failure operators actually see.
	hosts, err := monitor.ForceRefresh(false, 1000)
	assert.NoError(t, err)
	assert.NotEmpty(t, hosts)
}

// laggingDnsStrategy models a pod that starts up, or resets, *during* the
// switchover rather than after it: its topology cache is empty, so the monitor
// takes the pre-existing empty-cache fallback to the initial host — but the
// global endpoint's DNS has not followed the switchover yet, so that host
// answers as a reader in the demoted region. Once DNS catches up, the very same
// initial host becomes the promoted writer.
//
// The distinction that matters is the state this leaves behind: the monitor now
// holds a monitoring connection (to a non-writer) instead of none at all, which
// is still panic mode — isInPanicMode is `monitoringConn == nil ||
// !isVerifiedWriterConn`.
type laggingDnsStrategy struct {
	initialHost string
	staleHosts  []*host_info_util.HostInfo
	newHosts    []*host_info_util.HostInfo
	promoted    atomic.Bool
}

// dnsPinnedConn records what the initial host resolved to at the moment the
// connection was opened. A TCP connection does not follow DNS: one opened before
// the switchover keeps talking to the demoted region for its whole life, however
// the global endpoint resolves afterwards. Only a *new* connection sees the
// promotion — which is exactly why re-consulting the initial host has to mean
// dialing it again, not re-querying a connection already in hand.
type dnsPinnedConn struct {
	driver.Conn
	host     string
	promoted bool
}

func (s *laggingDnsStrategy) isPromotedInitialHost(conn driver.Conn) bool {
	pinned, ok := conn.(*dnsPinnedConn)
	return ok && pinned.host == s.initialHost && pinned.promoted
}

func (s *laggingDnsStrategy) QueryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	if s.isPromotedInitialHost(conn) {
		return s.newHosts, nil
	}
	return s.staleHosts, nil
}

func (s *laggingDnsStrategy) GetInstanceTemplate(string, driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (s *laggingDnsStrategy) IsWriterInstance(conn driver.Conn) (bool, error) {
	return s.isPromotedInitialHost(conn), nil
}

func (s *laggingDnsStrategy) GetInstanceId(conn driver.Conn) (string, string) {
	if s.isPromotedInitialHost(conn) {
		return s.newHosts[0].HostId, s.newHosts[0].GetHost()
	}
	return "", ""
}

func (s *laggingDnsStrategy) CreateHost(
	_, _ string, _ bool, _ int, _ time.Time, initialHost, _ *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return initialHost
}

// TestClusterTopologyMonitorRechecksInitialHostWhileHoldingNonWriterConn covers
// the case the other three miss. They all start from `monitoringConn == nil`,
// and their initial host reports as the writer on the very first recheck. But
// panic mode does not require a nil monitoring connection — an unverified one is
// enough — and openAnyConnectionAndUpdateTopology only dials the initial host
// `if c.loadConn(c.monitoringConn) == nil`. A monitor that is stalled while
// *holding* a connection to a non-writer therefore never re-consults the initial
// host at all; it re-queries the demoted region through the connection it
// already has.
//
// Reaching that state needs no contrivance: an empty cache sends the monitor
// down the pre-existing fallback, and a global endpoint whose DNS has not yet
// flipped hands it exactly such a connection.
func TestClusterTopologyMonitorRechecksInitialHostWhileHoldingNonWriterConn(t *testing.T) {
	originalInterval := driver_infrastructure.InitialHostRecheckIntervalNano
	driver_infrastructure.InitialHostRecheckIntervalNano = 100 * time.Millisecond
	defer func() { driver_infrastructure.InitialHostRecheckIntervalNano = originalInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHostInfo := staleHost(t, "cluster.global-mno.global.rds.amazonaws.com")
	staleHosts := []*host_info_util.HostInfo{
		staleHost(t, "old-region-instance-0.mno.us-east-1.rds.amazonaws.com"),
		staleHost(t, "old-region-instance-1.mno.us-east-1.rds.amazonaws.com"),
	}
	promotedHost := staleHost(t, "new-region-instance-0.pqr.us-west-2.rds.amazonaws.com")

	mockDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsNetworkError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().IsLoginError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER).AnyTimes()
	strategy := &laggingDnsStrategy{
		initialHost: initialHostInfo.GetHost(),
		staleHosts:  staleHosts,
		newHosts:    []*host_info_util.HostInfo{promotedHost},
	}

	// Each connection is pinned to whatever DNS said at the moment it was opened.
	mockPluginService.EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		DoAndReturn(func(hostInfo *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
			return &dnsPinnedConn{
				Conn:     &MockConn{},
				host:     hostInfo.GetHost(),
				promoted: strategy.promoted.Load(),
			}, nil
		}).
		AnyTimes()

	publisher := services.NewEventPublisher()
	storage := services.NewExpiringStorage(time.Minute, publisher)
	container := &services.FullServicesContainer{Storage: storage, Events: publisher}

	// Registered so the cache writes below actually land, but deliberately not
	// seeded: the empty cache is what drives the monitor to the initial host
	// before DNS has flipped.
	driver_infrastructure.TopologyStorageType.Register(storage)
	_, found := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
	require.False(t, found, "cache must start empty for the monitor to take the fallback path")

	monitor := driver_infrastructure.NewClusterTopologyMonitorImpl(
		container,
		staleTopologyClusterId,
		10*time.Millisecond,
		10*time.Millisecond,
		time.Minute,
		utils.NewRWMap[string, string](),
		initialHostInfo,
		initialHostInfo,
		mockPluginService,
		strategy,
	)

	monitor.Start()
	defer monitor.Stop()

	// The monitor falls back to the initial host, finds a non-writer, and caches
	// the demoted region's topology through that connection. Asserting this is
	// what proves the test reached the state it is about, rather than passing
	// from the nil-connection path the other tests already cover.
	require.Eventually(t, func() bool {
		topology, ok := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
		return ok && len(topology.GetHosts()) == len(staleHosts)
	}, 5*time.Second, 20*time.Millisecond,
		"monitor never cached the demoted region's topology, so it never held a connection to a non-writer")

	// DNS now follows the switchover: the initial host is the promoted writer.
	strategy.promoted.Store(true)

	assert.Eventually(t, func() bool {
		topology, ok := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
		if !ok {
			return false
		}
		for _, h := range topology.GetHosts() {
			if h.GetHost() == promotedHost.GetHost() {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond,
		"monitor kept re-querying the connection it already held to the demoted region and never re-consulted the initial host")
}

// trackedConn records whether a connection was ever closed, so a test can assert
// that the monitor did not drop one on the floor. identifiedConn cannot be reused
// for this: MockConn.closeCounter is a plain int written from whichever goroutine
// closes, which the race detector rejects.
type trackedConn struct {
	driver.Conn
	host   string
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// racingWriterStrategy models a host monitoring routine that finds a writer at the
// same moment the stalled monitor re-consults the initial host.
//
// A demoted instance can still answer as the writer for a short window after a
// switchover, so a host routine can reach the hand-off at cluster_topology_monitor.go:896-917:
// it publishes its connection into hostRoutinesWriterConn, sets its own local conn
// to nil so the deferred close skips it, and returns. Ownership has moved to the
// monitor at that point.
//
// The routine publishes the connection BEFORE it publishes the host info, with a
// topology query in between. Blocking that query holds the routine in exactly that
// window, which is what makes this test deterministic rather than a race to lose.
type racingWriterStrategy struct {
	initialHost     string
	staleWriterHost string
	staleHosts      []*host_info_util.HostInfo
	newHosts        []*host_info_util.HostInfo
	gate            chan struct{}
}

func (s *racingWriterStrategy) hostOf(conn driver.Conn) string {
	tracked, ok := conn.(*trackedConn)
	if !ok {
		return ""
	}
	return tracked.host
}

func (s *racingWriterStrategy) QueryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	switch s.hostOf(conn) {
	case s.initialHost:
		return s.newHosts, nil
	case s.staleWriterHost:
		// Hold the routine between publishing its connection and publishing its
		// host info, so the monitor cannot adopt it the ordinary way.
		<-s.gate
		return s.staleHosts, nil
	default:
		return s.staleHosts, nil
	}
}

func (s *racingWriterStrategy) GetInstanceTemplate(string, driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (s *racingWriterStrategy) IsWriterInstance(conn driver.Conn) (bool, error) {
	host := s.hostOf(conn)
	return host == s.initialHost || host == s.staleWriterHost, nil
}

func (s *racingWriterStrategy) GetInstanceId(conn driver.Conn) (string, string) {
	if s.hostOf(conn) == s.initialHost {
		return s.newHosts[0].HostId, s.newHosts[0].GetHost()
	}
	return "", ""
}

func (s *racingWriterStrategy) CreateHost(
	_, _ string, _ bool, _ int, _ time.Time, initialHost, _ *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return initialHost
}

// TestClusterTopologyMonitorDoesNotLeakHostRoutineWriterConn covers the ownership
// hand-off that the stall recheck opened up.
//
// Before the recheck existed, a monitor with host routines running could only leave
// panic mode through the adoption block at cluster_topology_monitor.go:209-226, which
// takes ownership of whatever the routines published — openAnyConnectionAndUpdateTopology
// was reachable only at :189, with no routines running. The regular-mode cleanup at
// :255-262 relies on that: it stops, waits for and clears the routines without ever
// closing hostRoutinesWriterConn, because by then the connection is supposed to be the
// monitoring connection.
//
// The recheck adds a second exit that does not adopt, so the cleanup can now run while
// a routine's published writer connection is still sitting there unowned. Nothing closes
// it: the routine set its local conn to nil, Close() at :309-319 only closes the
// monitoring connection, and re-entering panic mode overwrites the container at :181.
func TestClusterTopologyMonitorDoesNotLeakHostRoutineWriterConn(t *testing.T) {
	originalInterval := driver_infrastructure.InitialHostRecheckIntervalNano
	driver_infrastructure.InitialHostRecheckIntervalNano = 100 * time.Millisecond
	defer func() { driver_infrastructure.InitialHostRecheckIntervalNano = originalInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHostInfo := staleHost(t, "cluster.global-mno.global.rds.amazonaws.com")
	staleWriter := staleHost(t, "old-region-instance-0.mno.us-east-1.rds.amazonaws.com")
	staleHosts := []*host_info_util.HostInfo{
		staleWriter,
		staleHost(t, "old-region-instance-1.mno.us-east-1.rds.amazonaws.com"),
	}
	promotedHost := staleHost(t, "new-region-instance-0.pqr.us-west-2.rds.amazonaws.com")

	strategy := &racingWriterStrategy{
		initialHost:     initialHostInfo.GetHost(),
		staleWriterHost: staleWriter.GetHost(),
		staleHosts:      staleHosts,
		newHosts:        []*host_info_util.HostInfo{promotedHost},
		gate:            make(chan struct{}),
	}
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(strategy.gate) }) }

	var connMu sync.Mutex
	var publishedWriterConn *trackedConn

	mockDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsNetworkError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().IsLoginError(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()
	// The routine double-checks writer-ness through GetHostRole, so the demoted
	// instance has to answer WRITER there too to reach the hand-off.
	mockPluginService.EXPECT().
		GetHostRole(gomock.Any()).
		DoAndReturn(func(conn driver.Conn) host_info_util.HostRole {
			if tracked, ok := conn.(*trackedConn); ok && tracked.host == staleWriter.GetHost() {
				return host_info_util.WRITER
			}
			return host_info_util.READER
		}).
		AnyTimes()
	mockPluginService.EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		DoAndReturn(func(hostInfo *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
			conn := &trackedConn{Conn: &MockConn{}, host: hostInfo.GetHost()}
			if hostInfo.GetHost() == staleWriter.GetHost() {
				connMu.Lock()
				if publishedWriterConn == nil {
					publishedWriterConn = conn
				}
				connMu.Unlock()
			}
			return conn, nil
		}).
		AnyTimes()

	publisher := services.NewEventPublisher()
	storage := services.NewExpiringStorage(time.Minute, publisher)
	container := &services.FullServicesContainer{Storage: storage, Events: publisher}

	driver_infrastructure.TopologyStorageType.Register(storage)
	driver_infrastructure.TopologyStorageType.Set(
		storage, staleTopologyClusterId, driver_infrastructure.NewTopology(staleHosts))

	seeded, found := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
	require.True(t, found)
	require.Len(t, seeded.GetHosts(), len(staleHosts))

	monitor := driver_infrastructure.NewClusterTopologyMonitorImpl(
		container,
		staleTopologyClusterId,
		10*time.Millisecond,
		10*time.Millisecond,
		time.Minute,
		utils.NewRWMap[string, string](),
		initialHostInfo,
		initialHostInfo,
		mockPluginService,
		strategy,
	)

	monitor.Start()
	defer monitor.Stop()

	// Release the gate however the test exits, so a failed assertion cannot wedge
	// the run. This has to be registered AFTER monitor.Stop() so that it runs
	// BEFORE it: deferred calls run last-in-first-out, and Stop() ends in Close(),
	// which waits on the host routines. Releasing the gate afterwards would be too
	// late — the blocked routine could never finish and the wait would never return.
	defer releaseGate()

	// The routine has to have reached the hand-off before any of this means
	// anything, otherwise the test would pass without exercising the window.
	require.Eventually(t, func() bool {
		connMu.Lock()
		defer connMu.Unlock()
		return publishedWriterConn != nil
	}, 5*time.Second, 10*time.Millisecond,
		"no host routine ever connected to the demoted instance that reports as writer")

	// The monitor leaves panic mode through the recheck while that routine is still
	// held at the gate, so the ordinary adoption at :212 never sees a host info.
	require.Eventually(t, func() bool {
		topology, ok := driver_infrastructure.TopologyStorageType.Get(storage, staleTopologyClusterId)
		if !ok {
			return false
		}
		for _, h := range topology.GetHosts() {
			if h.GetHost() == promotedHost.GetHost() {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond,
		"monitor never recovered the promoted cluster, so the ownership window was never reached")

	// Let the routine finish and hand its connection over.
	releaseGate()

	connMu.Lock()
	leaked := publishedWriterConn
	connMu.Unlock()

	assert.Eventually(t, leaked.closed.Load, 5*time.Second, 20*time.Millisecond,
		"the writer connection a host routine published was never closed: the routine gave up "+
			"ownership, the monitor left panic mode without adopting it, and the regular-mode "+
			"cleanup cleared the routines without closing it")
}
