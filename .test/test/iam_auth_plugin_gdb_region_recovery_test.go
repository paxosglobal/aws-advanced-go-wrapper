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
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/iam"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedGdbRegionProvider answers lookup n (1-based) with script(n).
type scriptedGdbRegionProvider struct {
	calls  atomic.Int32
	script func(call int32) (region_util.Region, error)
}

func (p *scriptedGdbRegionProvider) GetRegion(
	_ string, _ *utils.RWMap[string, string], _ property_util.AwsWrapperProperty) (region_util.Region, error) {
	return p.script(p.calls.Add(1))
}

func useScriptedGdbRegionProvider(t *testing.T, script func(call int32) (region_util.Region, error)) *scriptedGdbRegionProvider {
	provider := &scriptedGdbRegionProvider{script: script}
	original := iam.GdbRegionProviderFactory
	iam.GdbRegionProviderFactory = func() region_util.RegionProvider { return provider }
	t.Cleanup(func() { iam.GdbRegionProviderFactory = original })
	return provider
}

// Other tests in this package leave a default logger that is not safe for concurrent use.
func useDiscardLogger(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(previous) })
}

func gdbPropsWithExpiration(seconds string) *utils.RWMap[string, string] {
	props := gdbProps()
	props.Put(property_util.IAM_EXPIRATION_SEC.Name, seconds)
	return props
}

func connectLoginError(_ *utils.RWMap[string, string]) (driver.Conn, error) {
	return nil, mysqlLoginError
}

func TestIamAuthPluginGdbRegionPanicReleasesWaiters(t *testing.T) {
	useDiscardLogger(t)
	synctest.Test(t, func(t *testing.T) {
		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		release := make(chan struct{})
		provider := useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
			if call == 1 {
				<-release
				panic("provider failed")
			}
			return region_util.US_EAST_1, nil
		})

		var wg sync.WaitGroup
		var leaderPanicked atomic.Bool
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { leaderPanicked.Store(recover() != nil) }()
			_, _ = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		}()
		synctest.Wait()

		var waiterErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, waiterErr = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		}()
		synctest.Wait()

		close(release)
		wg.Wait()

		assert.True(t, leaderPanicked.Load(), "the panic still reaches the caller that ran the lookup")
		require.Error(t, waiterErr)
		assert.Contains(t, waiterErr.Error(), "did not complete")

		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.NoError(t, err, "a later connection starts a new lookup")
		assert.Equal(t, int32(2), provider.calls.Load())
	})
}

func TestIamAuthPluginGdbRegionEmptyRegionIsAnError(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useScriptedGdbRegionProvider(t, func(int32) (region_util.Region, error) { return "", nil })

	for range 2 {
		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Unable to determine connection region")
	}
	assert.Equal(t, int32(2), provider.calls.Load(), "an empty region is not cached")
	assert.Empty(t, tokens.tokenRegions())
}

func TestIamAuthPluginGdbRegionEmptyRefreshKeepsLoginError(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
		if call == 1 {
			return region_util.US_EAST_1, nil
		}
		return "", nil
	})

	_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	require.NoError(t, err)

	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectLoginError)
	assert.Same(t, mysqlLoginError, err)
	assert.Equal(t, []region_util.Region{region_util.US_EAST_1}, tokens.tokenRegions(), "no token for an empty region")
}

func TestIamAuthPluginGdbRegionRefreshFailureKeepsLoginError(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
		if call == 1 {
			return region_util.US_EAST_1, nil
		}
		return "", errors.New("DescribeGlobalClusters throttled")
	})

	_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	require.NoError(t, err)

	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectLoginError)
	assert.Same(t, mysqlLoginError, err)
}

func TestIamAuthPluginGdbRegionNoRetryWhenRegionUnchangedAndTokenNew(t *testing.T) {
	tokens := &regionRecordingTokenUtility{}
	plugin := newGdbIamPlugin(t, tokens)
	provider := useScriptedGdbRegionProvider(t, func(int32) (region_util.Region, error) { return region_util.US_EAST_1, nil })

	_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
	require.NoError(t, err)
	iam.TokenCache.Clear()

	attempts := 0
	_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, func(*utils.RWMap[string, string]) (driver.Conn, error) {
		attempts++
		return nil, mysqlLoginError
	})
	assert.Same(t, mysqlLoginError, err)
	assert.Equal(t, 1, attempts, "a new token for an unchanged region is not retried")
	assert.Equal(t, int32(2), provider.calls.Load())
	assert.Equal(t, []region_util.Region{region_util.US_EAST_1, region_util.US_EAST_1}, tokens.tokenRegions())
}

// A lookup that started before a login was rejected can return the old writer.
// The refresh must not wait for it, and its late answer must not replace the
// refreshed region in the cache.
func TestIamAuthPluginGdbRegionRefreshDoesNotJoinOlderLookup(t *testing.T) {
	useDiscardLogger(t)
	synctest.Test(t, func(t *testing.T) {
		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		olderLookupGate := make(chan struct{})
		provider := useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
			switch call {
			case 1:
				return region_util.US_EAST_1, nil
			case 2:
				<-olderLookupGate
				return region_util.US_EAST_1, nil
			default:
				return region_util.US_WEST_2, nil
			}
		})
		props := func() *utils.RWMap[string, string] { return gdbPropsWithExpiration("1") }

		_, err := plugin.Connect(gdbHostInfo(t), props(), false, connectOK)
		require.NoError(t, err)

		// B connects with the cached us-east-1 region; its login is held open.
		bLogin := make(chan struct{})
		var bPasswords []string
		var bErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, bErr = plugin.Connect(gdbHostInfo(t), props(), false, func(p *utils.RWMap[string, string]) (driver.Conn, error) {
				password := property_util.GetVerifiedWrapperPropertyValue[string](p, property_util.PASSWORD)
				bPasswords = append(bPasswords, password)
				if len(bPasswords) == 1 {
					<-bLogin
				}
				if password != "token-"+string(region_util.US_WEST_2) {
					return nil, mysqlLoginError
				}
				return &MockConn{throwError: true}, nil
			})
		}()
		synctest.Wait()

		// The cache expires; A starts lookup 2, which will answer with the old writer.
		time.Sleep(2 * time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = plugin.Connect(gdbHostInfo(t), props(), false, connectOK)
		}()
		synctest.Wait()

		// B's login is rejected after the switchover. Its refresh must start lookup 3.
		close(bLogin)
		synctest.Wait()
		require.NoError(t, bErr)
		assert.Equal(t, []string{"token-us-east-1", "token-us-west-2"}, bPasswords)

		close(olderLookupGate)
		wg.Wait()
		assert.Equal(t, int32(3), provider.calls.Load())

		_, err = plugin.Connect(gdbHostInfo(t), props(), false, func(p *utils.RWMap[string, string]) (driver.Conn, error) {
			if property_util.GetVerifiedWrapperPropertyValue[string](p, property_util.PASSWORD) != "token-"+string(region_util.US_WEST_2) {
				return nil, mysqlLoginError
			}
			return &MockConn{throwError: true}, nil
		})
		require.NoError(t, err, "the older lookup's answer did not replace us-west-2 in the cache")
		assert.Equal(t, int32(3), provider.calls.Load())
	})
}

func TestGdbRegionProviderReusesRdsClientForDefaultCredentials(t *testing.T) {
	auth_helpers.ClearDefaultCredentialsProviders()
	t.Cleanup(auth_helpers.ClearDefaultCredentialsProviders)
	original := auth_helpers.NewRdsClient
	t.Cleanup(func() { auth_helpers.NewRdsClient = original })
	var created atomic.Int32
	auth_helpers.NewRdsClient = func(aws.CredentialsProvider) auth_helpers.RdsDescribeGlobalClustersClient {
		created.Add(1)
		return &MockRdsDescribeGlobalClustersClient{Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{{GlobalClusterMembers: []rdsTypes.GlobalClusterMember{{
				DBClusterArn: aws.String("arn:aws:rds:us-east-1:123456789012:cluster:writer"),
				IsWriter:     aws.Bool(true),
			}}}},
		}}
	}

	for range 3 {
		region, err := auth_helpers.NewGdbRegionProvider(nil).GetRegion(iamGdbWriterHost, emptyProps, property_util.IAM_REGION)
		require.NoError(t, err)
		assert.Equal(t, region_util.US_EAST_1, region)
	}
	assert.Equal(t, int32(1), created.Load())
}

// blockingRdsClient answers only when its context ends.
type blockingRdsClient struct{}

func (blockingRdsClient) DescribeGlobalClusters(
	ctx context.Context, _ *rds.DescribeGlobalClustersInput, _ ...func(*rds.Options)) (*rds.DescribeGlobalClustersOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGdbRegionProviderLookupTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		original := auth_helpers.GdbRegionLookupTimeout
		t.Cleanup(func() { auth_helpers.GdbRegionLookupTimeout = original })
		auth_helpers.GdbRegionLookupTimeout = 5 * time.Second

		provider := &auth_helpers.GdbRegionProvider{
			CredentialsProvider: aws.AnonymousCredentials{},
			RdsClientProvider: func(aws.CredentialsProvider) auth_helpers.RdsDescribeGlobalClustersClient {
				return blockingRdsClient{}
			},
		}
		start := time.Now()
		_, err := provider.GetRegion(iamGdbWriterHost, emptyProps, property_util.IAM_REGION)
		require.Error(t, err)
		// GdbRegionProvider wraps the cause as text, so match the message.
		assert.Contains(t, err.Error(), context.DeadlineExceeded.Error())
		assert.Equal(t, 5*time.Second, time.Since(start))
	})
}
