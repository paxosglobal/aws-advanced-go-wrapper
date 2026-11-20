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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connectWestOnly(p *utils.RWMap[string, string]) (driver.Conn, error) {
	if property_util.GetVerifiedWrapperPropertyValue[string](p, property_util.PASSWORD) != "token-"+string(region_util.US_WEST_2) {
		return nil, mysqlLoginError
	}
	return &MockConn{throwError: true}, nil
}

// Pool connections that are rejected together after a switchover share one
// lookup instead of each starting their own.
func TestIamAuthPluginGdbRegionConcurrentRefreshesShareOneLookup(t *testing.T) {
	useDiscardLogger(t)
	synctest.Test(t, func(t *testing.T) {
		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		refreshGate := make(chan struct{})
		provider := useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
			if call == 1 {
				return region_util.US_EAST_1, nil
			}
			<-refreshGate
			return region_util.US_WEST_2, nil
		})
		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.NoError(t, err)

		loginGate := make(chan struct{})
		const connections = 3
		errs := make([]error, connections)
		var wg sync.WaitGroup
		for i := range connections {
			wg.Add(1)
			go func() {
				defer wg.Done()
				first := true
				_, errs[i] = plugin.Connect(gdbHostInfo(t), gdbProps(), false, func(p *utils.RWMap[string, string]) (driver.Conn, error) {
					if first {
						first = false
						<-loginGate
					}
					return connectWestOnly(p)
				})
			}()
		}
		synctest.Wait()
		close(loginGate)
		synctest.Wait()
		close(refreshGate)
		wg.Wait()

		for _, err := range errs {
			assert.NoError(t, err)
		}
		assert.Equal(t, int32(2), provider.calls.Load(), "the three refreshes shared one lookup")
	})
}

// A lookup with no deadline of its own cannot hold a connection longer than
// GdbRegionWaitTimeout.
func TestIamAuthPluginGdbRegionWaitIsBounded(t *testing.T) {
	useDiscardLogger(t)
	synctest.Test(t, func(t *testing.T) {
		original := iam.GdbRegionWaitTimeout
		t.Cleanup(func() { iam.GdbRegionWaitTimeout = original })
		iam.GdbRegionWaitTimeout = 3 * time.Second

		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		release := make(chan struct{})
		useScriptedGdbRegionProvider(t, func(int32) (region_util.Region, error) {
			<-release
			return region_util.US_EAST_1, nil
		})

		start := time.Now()
		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timed out after 3s")
		assert.Equal(t, 3*time.Second, time.Since(start))

		// The lookup still finishes and fills the cache for the next connection.
		close(release)
		synctest.Wait()
		_, err = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		require.NoError(t, err)
	})
}

// A lookup that was running when the caches were cleared does not write its
// answer into the cleared cache.
func TestIamAuthPluginGdbRegionClearCachesDropsRunningLookup(t *testing.T) {
	useDiscardLogger(t)
	synctest.Test(t, func(t *testing.T) {
		tokens := &regionRecordingTokenUtility{}
		plugin := newGdbIamPlugin(t, tokens)
		release := make(chan struct{})
		provider := useScriptedGdbRegionProvider(t, func(call int32) (region_util.Region, error) {
			if call == 1 {
				<-release
				return region_util.US_EAST_1, nil
			}
			return region_util.US_WEST_2, nil
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = plugin.Connect(gdbHostInfo(t), gdbProps(), false, connectOK)
		}()
		synctest.Wait()
		iam.IamAuthPluginFactory{}.ClearCaches()
		close(release)
		wg.Wait()

		attempts := 0
		_, err := plugin.Connect(gdbHostInfo(t), gdbProps(), false, func(p *utils.RWMap[string, string]) (driver.Conn, error) {
			attempts++
			return connectWestOnly(p)
		})
		require.NoError(t, err)
		assert.Equal(t, 1, attempts, "the next connection looked the region up instead of using the pre-clear answer")
		assert.Equal(t, int32(2), provider.calls.Load())
	})
}

// credentialProcessProfile writes a config file whose profile gets temporary
// credentials from a script that counts its runs and expires them after expiresIn.
func credentialProcessProfile(t *testing.T, expiresIn time.Duration) (profile string, runs func() int) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	script := filepath.Join(dir, "credentials.sh")
	expiration := time.Now().Add(expiresIn).UTC().Format(time.RFC3339)
	require.NoError(t, os.WriteFile(script, []byte(fmt.Sprintf(
		"#!/bin/sh\necho run >> %q\necho '{\"Version\":1,\"AccessKeyId\":\"AKIATEST\",\"SecretAccessKey\":\"secret\",\"SessionToken\":\"token\",\"Expiration\":\"%s\"}'\n",
		counter, expiration)), 0o700))
	config := filepath.Join(dir, "config")
	require.NoError(t, os.WriteFile(config, []byte("[profile process]\ncredential_process = "+script+"\n"), 0o600))
	isolateAwsEnvironment(t, config, filepath.Join(dir, "credentials"))
	return "process", func() int {
		b, err := os.ReadFile(counter)
		if err != nil {
			return 0
		}
		return strings.Count(string(b), "run")
	}
}

func isolateAwsEnvironment(t *testing.T, configFile string, credentialsFile string) {
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_CONFIG_FILE", configFile)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credentialsFile)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	auth_helpers.ClearDefaultCredentialsProviders()
	t.Cleanup(auth_helpers.ClearDefaultCredentialsProviders)
}

func defaultProviderFor(t *testing.T, profile string) func() string {
	return func() string {
		provider, err := auth_helpers.GetAwsCredentialsProvider(*gdbHostInfo(t), map[string]string{property_util.AWS_PROFILE.Name: profile})
		require.NoError(t, err)
		credentials, err := provider.Retrieve(context.Background())
		require.NoError(t, err)
		return credentials.AccessKeyID
	}
}

// Credentials within DefaultCredentialsExpiryWindow of expiring are renewed, so a
// token signed with them stays valid for its whole lifetime.
func TestDefaultCredentialsRenewWithinExpiryWindow(t *testing.T) {
	profile, runs := credentialProcessProfile(t, 10*time.Minute)
	retrieve := defaultProviderFor(t, profile)

	retrieve()
	retrieve()
	assert.Equal(t, 2, runs(), "credentials expiring in 10 minutes are inside the 15 minute window")
}

// A rotated key in the credentials file is picked up after
// DefaultCredentialsProviderLifetime.
func TestDefaultCredentialsProviderReloadsAfterLifetime(t *testing.T) {
	dir := t.TempDir()
	credentialsFile := filepath.Join(dir, "credentials")
	writeKey := func(key string) {
		require.NoError(t, os.WriteFile(credentialsFile, []byte("[rotating]\naws_access_key_id = "+key+"\naws_secret_access_key = secret\n"), 0o600))
	}
	writeKey("AKIAOLDKEY")
	isolateAwsEnvironment(t, filepath.Join(dir, "config"), credentialsFile)
	original := auth_helpers.DefaultCredentialsProviderLifetime
	t.Cleanup(func() { auth_helpers.DefaultCredentialsProviderLifetime = original })
	auth_helpers.DefaultCredentialsProviderLifetime = 50 * time.Millisecond
	retrieve := defaultProviderFor(t, "rotating")

	assert.Equal(t, "AKIAOLDKEY", retrieve())
	writeKey("AKIANEWKEY")
	assert.Equal(t, "AKIAOLDKEY", retrieve(), "the provider is shared within its lifetime")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, "AKIANEWKEY", retrieve())
}
