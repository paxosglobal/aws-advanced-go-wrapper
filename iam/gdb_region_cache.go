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
	"sync"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

// GdbRegionProviderFactory creates the provider that resolves a global writer
// endpoint to the writer's region. Tests replace it.
var GdbRegionProviderFactory = func() region_util.RegionProvider {
	return auth_helpers.NewGdbRegionProvider(nil)
}

// GdbRegionCache holds the writer region resolved for each global writer endpoint.
// Without it, every connection called rds:DescribeGlobalClusters before it dialed.
var GdbRegionCache = utils.NewCache[string]()

type gdbRegionLookup struct {
	done   chan struct{}
	region region_util.Region
	err    error
}

var gdbRegionLookupsLock sync.Mutex

var gdbRegionLookups = map[string]*gdbRegionLookup{}

func gdbRegionCacheKey(host string, props *utils.RWMap[string, string]) string {
	return host + "|" + property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.AWS_PROFILE)
}

// getGdbRegion returns the cached writer region for host, or resolves it when
// there is no cached value or refresh is true. Concurrent resolutions for the
// same key share one call. The second return value is true when the region came
// from the cache.
func getGdbRegion(
	host string,
	props *utils.RWMap[string, string],
	expiration time.Duration,
	refresh bool) (region_util.Region, bool, error) {
	key := gdbRegionCacheKey(host, props)
	if !refresh {
		if region, ok := GdbRegionCache.Get(key); ok && region != "" {
			return region_util.Region(region), true, nil
		}
	}

	gdbRegionLookupsLock.Lock()
	if lookup, ok := gdbRegionLookups[key]; ok {
		gdbRegionLookupsLock.Unlock()
		<-lookup.done
		return lookup.region, false, lookup.err
	}
	lookup := &gdbRegionLookup{done: make(chan struct{})}
	gdbRegionLookups[key] = lookup
	gdbRegionLookupsLock.Unlock()

	lookup.region, lookup.err = GdbRegionProviderFactory().GetRegion(host, props, property_util.IAM_REGION)
	if lookup.err == nil && lookup.region != "" {
		GdbRegionCache.Put(key, string(lookup.region), expiration)
	}

	gdbRegionLookupsLock.Lock()
	delete(gdbRegionLookups, key)
	gdbRegionLookupsLock.Unlock()
	close(lookup.done)

	return lookup.region, false, lookup.err
}

func invalidateGdbRegion(host string, props *utils.RWMap[string, string]) {
	GdbRegionCache.Remove(gdbRegionCacheKey(host, props))
}
