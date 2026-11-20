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
	"errors"
	"sync"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

// GdbRegionProviderFactory creates the provider that resolves a global writer
// endpoint to the writer's region. Tests replace it.
var GdbRegionProviderFactory = func() region_util.RegionProvider {
	return auth_helpers.NewGdbRegionProvider(nil)
}

// A cached writer region and the lookup that produced it. An entry with an empty
// region marks a region that was rejected: lookups that started before the
// rejection cannot replace it.
type gdbRegionEntry struct {
	region    region_util.Region
	lookupSeq uint64
	expiresAt time.Time
}

type gdbRegionLookup struct {
	seq    uint64
	done   chan struct{}
	region region_util.Region
	err    error
}

// One lock covers the cache and the lookups in flight, so a cache check, the
// decision to join or start a lookup, and every cache write are atomic with
// respect to each other. It is never held during a lookup.
var (
	gdbRegionLock    sync.Mutex
	gdbRegionSeq     uint64
	gdbRegionCache   = map[string]*gdbRegionEntry{}
	gdbRegionLookups = map[string]*gdbRegionLookup{}
)

func gdbRegionCacheKey(host string, props *utils.RWMap[string, string]) string {
	return host + "|" + property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.AWS_PROFILE)
}

func clearGdbRegionCache() {
	gdbRegionLock.Lock()
	defer gdbRegionLock.Unlock()
	gdbRegionCache = map[string]*gdbRegionEntry{}
	gdbRegionLookups = map[string]*gdbRegionLookup{}
}

func freshGdbRegionLocked(key string) (region_util.Region, bool) {
	entry := gdbRegionCache[key]
	if entry == nil || entry.region == "" || !time.Now().Before(entry.expiresAt) {
		return "", false
	}
	return entry.region, true
}

// getGdbRegion returns the writer region of host: the cached value while it is
// fresh, otherwise the result of a lookup that concurrent callers share. The
// second return value is true when the region came from the cache.
func getGdbRegion(
	host string,
	props *utils.RWMap[string, string],
	expiration time.Duration) (region_util.Region, bool, error) {
	key := gdbRegionCacheKey(host, props)

	gdbRegionLock.Lock()
	if region, ok := freshGdbRegionLocked(key); ok {
		gdbRegionLock.Unlock()
		return region, true, nil
	}
	lookup, leader := gdbRegionLookups[key], false
	if lookup == nil {
		lookup, leader = startGdbRegionLookupLocked(key), true
	}
	gdbRegionLock.Unlock()

	if leader {
		runGdbRegionLookup(key, host, props, expiration, lookup)
	}
	<-lookup.done
	return lookup.region, false, lookup.err
}

// refreshGdbRegion resolves the writer region of host again after a login with
// rejectedRegion failed. If another caller has already cached a different
// region, it returns that region. Otherwise it waits only for a lookup that
// started after the rejection, so the answer cannot predate it.
func refreshGdbRegion(
	host string,
	props *utils.RWMap[string, string],
	expiration time.Duration,
	rejectedRegion region_util.Region) (region_util.Region, error) {
	key := gdbRegionCacheKey(host, props)

	gdbRegionLock.Lock()
	if region, ok := freshGdbRegionLocked(key); ok && region != rejectedRegion {
		gdbRegionLock.Unlock()
		return region, nil
	}
	rejectedAt := gdbRegionSeq
	gdbRegionCache[key] = &gdbRegionEntry{lookupSeq: rejectedAt}
	lookup, leader := gdbRegionLookups[key], false
	if lookup == nil || lookup.seq <= rejectedAt {
		lookup, leader = startGdbRegionLookupLocked(key), true
	}
	gdbRegionLock.Unlock()

	if leader {
		runGdbRegionLookup(key, host, props, expiration, lookup)
	}
	<-lookup.done
	return lookup.region, lookup.err
}

func startGdbRegionLookupLocked(key string) *gdbRegionLookup {
	gdbRegionSeq++
	lookup := &gdbRegionLookup{seq: gdbRegionSeq, done: make(chan struct{})}
	gdbRegionLookups[key] = lookup
	return lookup
}

// runGdbRegionLookup resolves the region and publishes it. The deferred block
// runs even if the provider panics, so callers waiting on done are released
// with an error instead of blocking forever; the panic still reaches the caller
// that ran the lookup.
func runGdbRegionLookup(
	key string,
	host string,
	props *utils.RWMap[string, string],
	expiration time.Duration,
	lookup *gdbRegionLookup) {
	completed := false
	defer func() {
		if !completed {
			lookup.region = ""
			lookup.err = errors.New(error_util.GetMessage("IamAuthPlugin.gdbRegionLookupIncomplete", host))
		}
		gdbRegionLock.Lock()
		if gdbRegionLookups[key] == lookup {
			delete(gdbRegionLookups, key)
		}
		if lookup.err == nil {
			if entry := gdbRegionCache[key]; entry == nil || entry.lookupSeq < lookup.seq {
				gdbRegionCache[key] = &gdbRegionEntry{
					region:    lookup.region,
					lookupSeq: lookup.seq,
					expiresAt: time.Now().Add(expiration),
				}
			}
		}
		gdbRegionLock.Unlock()
		close(lookup.done)
	}()

	region, err := GdbRegionProviderFactory().GetRegion(host, props, property_util.IAM_REGION)
	if err == nil && region == "" {
		err = errors.New(error_util.GetMessage("IamAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}
	lookup.region, lookup.err = region, err
	completed = true
}
