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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/iam"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const iamGdbWriterHost = "my-gdb.global-abc123.global.rds.amazonaws.com"

// countingGdbRegionProvider stands in for the rds:DescribeGlobalClusters lookup.
type countingGdbRegionProvider struct {
	calls   atomic.Int32
	region  atomic.Value
	err     atomic.Pointer[error]
	release chan struct{}
}

func (p *countingGdbRegionProvider) GetRegion(
	_ string, _ *utils.RWMap[string, string], _ property_util.AwsWrapperProperty) (region_util.Region, error) {
	p.calls.Add(1)
	if p.release != nil {
		<-p.release
	}
	if err := p.err.Load(); err != nil {
		return "", *err
	}
	return p.region.Load().(region_util.Region), nil
}

func useCountingGdbRegionProvider(t *testing.T, region region_util.Region) *countingGdbRegionProvider {
	provider := &countingGdbRegionProvider{}
	provider.region.Store(region)
	original := iam.GdbRegionProviderFactory
	iam.GdbRegionProviderFactory = func() region_util.RegionProvider { return provider }
	t.Cleanup(func() { iam.GdbRegionProviderFactory = original })
	return provider
}

// regionRecordingTokenUtility is safe for concurrent use and records the region of each token.
type regionRecordingTokenUtility struct {
	mu      sync.Mutex
	regions []region_util.Region
}

func (u *regionRecordingTokenUtility) GenerateAuthenticationToken(
	_ string, _ string, _ int, region region_util.Region,
	_ aws.CredentialsProvider, _ driver_infrastructure.PluginService) (string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.regions = append(u.regions, region)
	return "token-" + string(region), nil
}

func (u *regionRecordingTokenUtility) tokenRegions() []region_util.Region {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]region_util.Region(nil), u.regions...)
}

func gdbProps() *utils.RWMap[string, string] {
	return MakeMapFromKeysAndVals(
		property_util.USER.Name, "someUser",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
}

func gdbHostInfo(t *testing.T) *host_info_util.HostInfo {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(iamGdbWriterHost).SetPort(1234).Build()
	require.NoError(t, err)
	return hostInfo
}

func newGdbIamPlugin(t *testing.T, tokens auth_helpers.IamTokenUtility) *iam.IamAuthPlugin {
	servicesContainer, _ := beforeIamAuthPluginTests(t, gdbProps())
	plugin, err := iam.NewIamAuthPlugin(servicesContainer, tokens, gdbProps())
	require.NoError(t, err)
	return plugin
}

func connectOK(_ *utils.RWMap[string, string]) (driver.Conn, error) {
	return &MockConn{throwError: true}, nil
}

var mysqlLoginError = &mysql.MySQLError{SQLState: [5]byte(([]byte(mysql_driver.SqlStateAccessError))[:5])}

func TestIamAuthPluginGdbRegionIsCachedAcrossConnections(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useCountingGdbRegionProvider(t, region_util.US_EAST_1)

	for range 3 {
		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), provider.calls.Load())
	assert.Equal(t, []region_util.Region{region_util.US_EAST_1}, tokens.tokenRegions())
}

func TestIamAuthPluginGdbRegionConcurrentConnectionsShareOneLookup(t *testing.T) {
	// Other tests in this package leave a default logger that is not safe for concurrent use.
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	synctest.Test(t, func(t *testing.T) {
		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		provider := useCountingGdbRegionProvider(t, region_util.US_EAST_1)
		provider.release = make(chan struct{})

		const connections = 20
		var wg sync.WaitGroup
		errs := make([]error, connections)
		for i := range connections {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
			}()
		}
		// Every goroutine is now in the lookup or waiting for it.
		synctest.Wait()
		close(provider.release)
		wg.Wait()

		for _, err := range errs {
			assert.NoError(t, err)
		}
		assert.Equal(t, int32(1), provider.calls.Load())
		for _, region := range tokens.tokenRegions() {
			assert.Equal(t, region_util.US_EAST_1, region)
		}
	})
}

func TestIamAuthPluginGdbRegionResolvedAgainAfterLoginFailure(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useCountingGdbRegionProvider(t, region_util.US_EAST_1)

	_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	require.NoError(t, err)

	// Switchover: the writer is now in us-west-2, so a token for us-east-1 is rejected.
	provider.region.Store(region_util.US_WEST_2)
	var passwords []string
	connectWestOnly := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		password := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD)
		passwords = append(passwords, password)
		if password != "token-"+string(region_util.US_WEST_2) {
			return nil, mysqlLoginError
		}
		return &MockConn{throwError: true}, nil
	}

	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectWestOnly)
	require.NoError(t, err)
	assert.Equal(t, []string{"token-us-east-1", "token-us-west-2"}, passwords)
	assert.Equal(t, int32(2), provider.calls.Load())

	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectWestOnly)
	require.NoError(t, err)
	assert.Equal(t, int32(2), provider.calls.Load(), "the refreshed region is cached")
	assert.Equal(t, []region_util.Region{region_util.US_EAST_1, region_util.US_WEST_2}, tokens.tokenRegions())
}

func TestIamAuthPluginGdbRegionNotCachedWhenLookupFails(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useCountingGdbRegionProvider(t, region_util.US_EAST_1)
	lookupErr := errors.New("DescribeGlobalClusters throttled")
	provider.err.Store(&lookupErr)

	_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	assert.ErrorIs(t, err, lookupErr)

	provider.err.Store(nil)
	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	require.NoError(t, err)
	assert.Equal(t, int32(2), provider.calls.Load())
}

func TestIamAuthPluginGdbRegionPropSkipsLookup(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useCountingGdbRegionProvider(t, region_util.US_EAST_1)
	props := gdbProps()
	props.Put(property_util.IAM_REGION.Name, string(region_util.US_WEST_2))

	_, err := plugin.Connect(gdbHostInfo(t), props, false, connectOK)
	require.NoError(t, err)
	assert.Equal(t, int32(0), provider.calls.Load())
	assert.Equal(t, []region_util.Region{region_util.US_WEST_2}, tokens.tokenRegions())
}

func TestGetAwsCredentialsProviderReusesDefaultProvider(t *testing.T) {
	auth_helpers.ClearDefaultCredentialsProviders()
	t.Cleanup(auth_helpers.ClearDefaultCredentialsProviders)
	hostInfo := gdbHostInfo(t)

	first, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, map[string]string{})
	require.NoError(t, err)
	second, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, map[string]string{})
	require.NoError(t, err)

	assert.Same(t, first, second)
}
