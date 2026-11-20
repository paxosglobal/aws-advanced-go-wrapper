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
	"fmt"
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

// GdbRegionWaitTimeout bounds how long a connection waits for the writer region.
// It applies even when the region lookup itself has no deadline (auth-helpers
// before the lookup deadline was added); the lookup keeps running and publishes
// its result when it finishes.
var GdbRegionWaitTimeout = 15 * time.Second

// A cached writer region and the lookup that produced it. An entry with an empty
// region marks a rejected region: lookups that started before the marker cannot
// replace it, and a refresh joins only a lookup that started after it.
type gdbRegionEntry struct {
	region    region_util.Region
	lookupSeq uint64
	expiresAt time.Time
}

type gdbRegionLookup struct {
	seq        uint64
	generation uint64
	done       chan struct{}
	region     region_util.Region
	err        error
}

// One lock covers the cache and the lookups in flight, so a cache check, the
// decision to join or start a lookup, and every cache write are atomic with
// respect to each other. It is never held during a lookup. generation changes
// on clearGdbRegionCache, so a lookup that was running then does not write into
// the cleared cache.
var (
	gdbRegionLock       sync.Mutex
	gdbRegionSeq        uint64
	gdbRegionGeneration uint64
	gdbRegionCache      = map[string]*gdbRegionEntry{}
	gdbRegionLookups    = map[string]*gdbRegionLookup{}
)

func gdbRegionCacheKey(host string, props *utils.RWMap[string, string]) string {
	return host + "|" + property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.AWS_PROFILE)
}

func clearGdbRegionCache() {
	gdbRegionLock.Lock()
	defer gdbRegionLock.Unlock()
	gdbRegionGeneration++
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
func getGdbRegion(host string, props *utils.RWMap[string, string]) (region_util.Region, bool, error) {
	key := gdbRegionCacheKey(host, props)

	gdbRegionLock.Lock()
	if region, ok := freshGdbRegionLocked(key); ok {
		gdbRegionLock.Unlock()
		return region, true, nil
	}
	lookup := gdbRegionLookups[key]
	if lookup == nil {
		lookup = startGdbRegionLookupLocked(key, host, props)
	}
	gdbRegionLock.Unlock()

	region, err := waitForGdbRegion(host, lookup)
	return region, false, err
}

// refreshGdbRegion resolves the writer region of host again after a login with
// rejectedRegion failed. If another caller has already cached a different
// region, it returns that region. If a lookup started after the latest
// rejection marker is in flight, it waits for that lookup. Otherwise it marks
// the region rejected and starts a lookup, so the answer cannot predate the
// rejection.
func refreshGdbRegion(
	host string,
	props *utils.RWMap[string, string],
	rejectedRegion region_util.Region) (region_util.Region, error) {
	key := gdbRegionCacheKey(host, props)

	gdbRegionLock.Lock()
	if region, ok := freshGdbRegionLocked(key); ok && region != rejectedRegion {
		gdbRegionLock.Unlock()
		return region, nil
	}
	entry, lookup := gdbRegionCache[key], gdbRegionLookups[key]
	joinable := lookup != nil && entry != nil && entry.region == "" && lookup.seq > entry.lookupSeq
	if !joinable {
		gdbRegionCache[key] = &gdbRegionEntry{lookupSeq: gdbRegionSeq}
		lookup = startGdbRegionLookupLocked(key, host, props)
	}
	gdbRegionLock.Unlock()

	return waitForGdbRegion(host, lookup)
}

func waitForGdbRegion(host string, lookup *gdbRegionLookup) (region_util.Region, error) {
	timer := time.NewTimer(GdbRegionWaitTimeout)
	defer timer.Stop()
	select {
	case <-lookup.done:
		return lookup.region, lookup.err
	case <-timer.C:
		return "", errors.New(error_util.GetMessage("IamAuthPlugin.gdbRegionLookupTimedOut", host, GdbRegionWaitTimeout))
	}
}

// startGdbRegionLookupLocked registers a lookup and runs it on its own goroutine,
// so every caller's wait can be bounded. The deferred block releases waiters and
// publishes the result even if the provider panics; the panic becomes the
// lookup's error instead of crashing the process from a goroutine.
func startGdbRegionLookupLocked(key string, host string, props *utils.RWMap[string, string]) *gdbRegionLookup {
	gdbRegionSeq++
	lookup := &gdbRegionLookup{seq: gdbRegionSeq, generation: gdbRegionGeneration, done: make(chan struct{})}
	gdbRegionLookups[key] = lookup
	lookupProps := utils.NewRWMapFromCopy(props)

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				lookup.region = ""
				lookup.err = errors.New(error_util.GetMessage("IamAuthPlugin.gdbRegionLookupIncomplete", host, fmt.Sprint(recovered)))
			}
			gdbRegionLock.Lock()
			if lookup.generation == gdbRegionGeneration {
				if gdbRegionLookups[key] == lookup {
					delete(gdbRegionLookups, key)
				}
				if lookup.err == nil {
					if entry := gdbRegionCache[key]; entry == nil || entry.lookupSeq < lookup.seq {
						expiration := time.Duration(property_util.GetExpirationValue(lookupProps, property_util.IAM_EXPIRATION_SEC)) * time.Second
						gdbRegionCache[key] = &gdbRegionEntry{
							region:    lookup.region,
							lookupSeq: lookup.seq,
							expiresAt: time.Now().Add(expiration),
						}
					}
				}
			}
			gdbRegionLock.Unlock()
			close(lookup.done)
		}()

		region, err := GdbRegionProviderFactory().GetRegion(host, lookupProps, property_util.IAM_REGION)
		if err == nil && region == "" {
			err = errors.New(error_util.GetMessage("IamAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
		}
		lookup.region, lookup.err = region, err
	}()
	return lookup
}
