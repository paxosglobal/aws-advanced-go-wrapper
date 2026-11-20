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

package auth_helpers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type AwsCredentialsProviderHandler interface {
	GetAwsCredentialsProvider(hostInfo host_info_util.HostInfo, props map[string]string) (aws.CredentialsProvider, error)
}

var awsCredentialsProviderHandler AwsCredentialsProviderHandler

var awsCredentialsProviderManageLock sync.RWMutex

func GetAwsCredentialsProvider(hostInfo host_info_util.HostInfo, props map[string]string) (aws.CredentialsProvider, error) {
	awsCredentialsProviderManageLock.RLock()
	defer awsCredentialsProviderManageLock.RUnlock()
	if awsCredentialsProviderHandler != nil {
		return awsCredentialsProviderHandler.GetAwsCredentialsProvider(hostInfo, props)
	}
	return getDefaultProvider(property_util.GetVerifiedWrapperPropertyValueFromMap[string](props, property_util.AWS_PROFILE))
}

func SetAwsCredentialsProviderHandler(credentialsHandler AwsCredentialsProviderHandler) {
	awsCredentialsProviderManageLock.Lock()
	defer awsCredentialsProviderManageLock.Unlock()
	awsCredentialsProviderHandler = credentialsHandler
}

type defaultCredentials struct {
	provider  aws.CredentialsProvider
	rdsClient RdsDescribeGlobalClustersClient
	createdAt time.Time
}

var defaultProviders = map[string]*defaultCredentials{}

var defaultProvidersLock sync.Mutex

// DefaultCredentialsProviderLifetime is how long getDefaultProvider shares one
// provider before it loads the default configuration again, so rotated keys in
// a credentials file or the environment are picked up.
var DefaultCredentialsProviderLifetime = 15 * time.Minute

// DefaultCredentialsExpiryWindow makes a shared provider renew temporary
// credentials this long before they expire. It must be longer than the IAM
// token lifetime (iamExpirationSec, 870s by default): RDS requires the
// credentials that signed a token to be valid when the token is used.
var DefaultCredentialsExpiryWindow = 15 * time.Minute

// getDefaultProvider returns a provider per profile that is shared for
// DefaultCredentialsProviderLifetime. LoadDefaultConfig wraps the provider in an
// aws.CredentialsCache, so a shared provider reuses credentials until they are
// within DefaultCredentialsExpiryWindow of expiring. With a new provider per
// call, each IAM connection fetched new credentials (an STS call under IRSA).
func getDefaultProvider(profile string) (aws.CredentialsProvider, error) {
	defaultProvidersLock.Lock()
	defer defaultProvidersLock.Unlock()
	if entry, ok := defaultProviders[profile]; ok && time.Since(entry.createdAt) < DefaultCredentialsProviderLifetime {
		return entry.provider, nil
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(profile),
		config.WithCredentialsCacheOptions(func(o *aws.CredentialsCacheOptions) {
			o.ExpiryWindow = DefaultCredentialsExpiryWindow
		}))
	if err != nil {
		return nil, err
	}
	defaultProviders[profile] = &defaultCredentials{provider: cfg.Credentials, createdAt: time.Now()}
	return cfg.Credentials, nil
}

// sharedRdsClient returns one RDS client per provider that getDefaultProvider
// shares, and nil for any other provider. Only *aws.CredentialsCache values are
// compared, so a provider of a type that is not comparable cannot panic here.
func sharedRdsClient(credentialsProvider aws.CredentialsProvider) RdsDescribeGlobalClustersClient {
	cache, ok := credentialsProvider.(*aws.CredentialsCache)
	if !ok || cache == nil {
		return nil
	}
	defaultProvidersLock.Lock()
	defer defaultProvidersLock.Unlock()
	for _, entry := range defaultProviders {
		if shared, ok := entry.provider.(*aws.CredentialsCache); ok && shared == cache {
			if entry.rdsClient == nil {
				entry.rdsClient = NewRdsClient(credentialsProvider)
			}
			return entry.rdsClient
		}
	}
	return nil
}

// ClearDefaultCredentialsProviders drops the providers and RDS clients that
// getDefaultProvider and sharedRdsClient share.
func ClearDefaultCredentialsProviders() {
	defaultProvidersLock.Lock()
	defer defaultProvidersLock.Unlock()
	defaultProviders = map[string]*defaultCredentials{}
}
