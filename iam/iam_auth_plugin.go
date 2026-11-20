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

package iam

import (
	"database/sql/driver"
	"errors"
	"log/slog"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
)

func init() {
	awssql.UsePluginFactory(driver_infrastructure.IAM_PLUGIN_CODE,
		NewIamAuthPluginFactory())
}

type IamAuthPluginFactory struct{}

func (factory IamAuthPluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewIamAuthPlugin(servicesContainer, &auth_helpers.RegularIamTokenUtility{}, props)
}

func (factory IamAuthPluginFactory) ClearCaches() {
	TokenCache.Clear()
	GdbRegionCache.Clear()
	auth_helpers.ClearDefaultCredentialsProviders()
}

func NewIamAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return IamAuthPluginFactory{}
}

var TokenCache = utils.NewCache[string]()

type IamAuthPlugin struct {
	plugins.BaseConnectionPlugin
	servicesContainer driver_infrastructure.ServicesContainer
	iamTokenUtility   auth_helpers.IamTokenUtility
	props             *utils.RWMap[string, string]
	fetchTokenCounter telemetry.TelemetryCounter
}

func (iamAuthPlugin *IamAuthPlugin) GetPluginCode() string {
	return driver_infrastructure.IAM_PLUGIN_CODE
}

func NewIamAuthPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	iamTokenUtility auth_helpers.IamTokenUtility,
	props *utils.RWMap[string, string]) (*IamAuthPlugin, error) {
	pluginService := servicesContainer.GetPluginService()
	fetchTokenCounter, err := pluginService.GetTelemetryFactory().CreateCounter("iam.fetchToken.count")
	if err != nil {
		return nil, err
	}

	return &IamAuthPlugin{
		servicesContainer: servicesContainer,
		props:             props,
		iamTokenUtility:   iamTokenUtility,
		fetchTokenCounter: fetchTokenCounter,
	}, nil
}

func (iamAuthPlugin *IamAuthPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (iamAuthPlugin *IamAuthPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return iamAuthPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (iamAuthPlugin *IamAuthPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return iamAuthPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (iamAuthPlugin *IamAuthPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	host := auth_helpers.GetIamHost(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST), *hostInfo)

	port := auth_helpers.GetIamPort(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		*hostInfo, iamAuthPlugin.servicesContainer.GetPluginService().GetDialect().GetDefaultPort())

	// The region of a global writer endpoint is the current writer's region. It is
	// cached as long as a token is, and resolved again when a login with it fails,
	// because a switchover can move the writer to another region.
	resolvesGdbRegion := utils.IsGlobalDbWriterClusterDns(host) &&
		region_util.GetRegionFromProps(props, property_util.IAM_REGION) == ""
	gdbRegionExpiration := time.Duration(property_util.GetExpirationValue(props, property_util.IAM_EXPIRATION_SEC)) * time.Second

	var region region_util.Region
	var regionFromCache bool
	var regionErr error
	if resolvesGdbRegion {
		region, regionFromCache, regionErr = getGdbRegion(host, props, gdbRegionExpiration, false)
		if regionFromCache {
			slog.Debug(error_util.GetMessage("IamAuthPlugin.useCachedRegion", region, host))
		}
	} else {
		region, regionErr = (&region_util.DefaultRegionProvider{}).GetRegion(host, props, property_util.IAM_REGION)
	}
	if regionErr != nil {
		return nil, regionErr
	}
	if region == "" {
		return nil, errors.New(error_util.GetMessage("IamAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	user := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER)
	cacheKey := auth_helpers.GetCacheKey(user, host, port, region)

	token, cachedTokenFound := TokenCache.Get(cacheKey)
	isCachedToken := cachedTokenFound && token != ""
	if isCachedToken {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.useCachedToken"))
		property_util.PASSWORD.Set(props, token)
	} else {
		err := iamAuthPlugin.fetchAndSetToken(hostInfo, host, port, region, cacheKey, props)
		if err != nil {
			return nil, err
		}
	}

	conn, err := connectFunc(props)
	if err == nil {
		return conn, nil
	} else {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.connectionError", hostInfo.GetHost(), err))
		if !iamAuthPlugin.servicesContainer.GetPluginService().IsLoginError(err) || !(isCachedToken || regionFromCache) {
			return nil, err
		}
	}

	// Login unsuccessful with a cached token or a cached region.
	// Resolve the region again if it was cached, generate a new token and connect again.
	if regionFromCache {
		cachedRegion := region
		invalidateGdbRegion(host, props)
		region, _, regionErr = getGdbRegion(host, props, gdbRegionExpiration, true)
		if regionErr != nil {
			return nil, regionErr
		}
		slog.Info(error_util.GetMessage("IamAuthPlugin.cachedRegionLoginError", host, cachedRegion, err, region))
		cacheKey = auth_helpers.GetCacheKey(user, host, port, region)
	}
	err = iamAuthPlugin.fetchAndSetToken(hostInfo, host, port, region, cacheKey, props)
	if err != nil {
		return nil, err
	}

	return connectFunc(props)
}

func (iamAuthPlugin *IamAuthPlugin) fetchAndSetToken(
	hostInfo *host_info_util.HostInfo,
	host string,
	port int,
	region region_util.Region,
	cacheKey string,
	props *utils.RWMap[string, string]) error {
	tokenExpirationSec := property_util.GetExpirationValue(props, property_util.IAM_EXPIRATION_SEC)
	awsCredentialsProvider, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, props.GetAllEntries())
	if err != nil {
		slog.Error(error_util.GetMessage("IamAuthPlugin.errorGettingAwsCredentialsProvider", err))
		return err
	}
	iamAuthPlugin.fetchTokenCounter.Inc(iamAuthPlugin.servicesContainer.GetPluginService().GetTelemetryContext())
	token, err := iamAuthPlugin.iamTokenUtility.GenerateAuthenticationToken(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		host,
		port,
		region,
		awsCredentialsProvider,
		iamAuthPlugin.servicesContainer.GetPluginService())
	if err != nil || token == "" {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.errorGeneratingNewToken", err))
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken"))
	property_util.PASSWORD.Set(props, token)
	TokenCache.Put(cacheKey, token, time.Duration(tokenExpirationSec)*time.Second)
	return nil
}
