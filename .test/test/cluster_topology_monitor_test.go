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
