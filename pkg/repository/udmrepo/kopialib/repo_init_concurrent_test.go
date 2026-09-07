/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kopialib

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/cockroachdb/errors"
	"github.com/kopia/kopia/repo/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo/kopialib/backend"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// concurrentStoreCount is the number of goroutines that concurrently acquire
// backend stores in the tests below. The fix for #10344 turned backendStores
// into a factory (newStore func()) precisely so that concurrently executing
// controllers never share one store instance; these tests pin that contract.
const concurrentStoreCount = 32

// concurrentMarkerKey is the storage option key used to tag each goroutine's
// Setup call so per-instance state interference can be detected.
const concurrentMarkerKey = "velero-bughunt-concurrent-marker"

// countingBackendStore is a fake backend.Store recording per-instance Setup
// activity. setupCount/setupMarker are deliberately non-atomic: in these tests
// each instance is owned by exactly one goroutine, so plain field access is
// race-free when findBackendStore keeps returning fresh instances per call.
// If the factory contract regresses back to sharing one store instance across
// callers (#10344), the concurrent writes to these fields trip the race
// detector under `go test -race`, making this file a standing regression trap.
type countingBackendStore struct {
	setupCount  int
	setupMarker string
}

func (s *countingBackendStore) Setup(ctx context.Context, flags map[string]string, logger logrus.FieldLogger) error {
	s.setupCount++
	s.setupMarker = flags[concurrentMarkerKey]
	return nil
}

func (s *countingBackendStore) Connect(ctx context.Context, isCreate bool, logger logrus.FieldLogger) (blob.Storage, error) {
	return nil, errors.New("Connect should not be called by findBackendStore/setupBackendStore concurrency tests")
}

// replaceBackendStoresWithCountingFactory swaps the package-level backendStores
// for a single counting-fake factory entry and returns a restore function to be
// deferred. The replacement happens before any test goroutine is started, so it
// is race-free; this mirrors the backendStores replacement pattern already used
// by TestCreateBackupRepo and friends in repo_init_test.go.
func replaceBackendStoresWithCountingFactory() func() {
	original := backendStores
	backendStores = []kopiaBackendStoreFactory{
		{udmrepo.StorageTypeS3, "counting fake store", func() backend.Store { return &countingBackendStore{} }},
	}

	return func() {
		backendStores = original
	}
}

// TestFindBackendStoreConcurrent pins the concurrency contract behind #10344:
// N goroutines acquiring the same storage type concurrently must each receive
// their own kopiaBackendStore wrapper and their own backend.Store instance, and
// each instance must only ever observe the Setup call of its own acquirer.
// The pre-existing TestFindBackendStore only pins sequential (2-call)
// distinctness; a regression to value sharing would only surface here under
// `go test -race`.
func TestFindBackendStoreConcurrent(t *testing.T) {
	t.Run("returns distinct store instances when called concurrently", func(t *testing.T) {
		restore := replaceBackendStoresWithCountingFactory()
		defer restore()

		wrappers := make([]*kopiaBackendStore, concurrentStoreCount)

		var start, done sync.WaitGroup
		start.Add(concurrentStoreCount)
		done.Add(concurrentStoreCount)

		for i := 0; i < concurrentStoreCount; i++ {
			go func(idx int) {
				defer done.Done()
				start.Done()
				start.Wait() // release all goroutines together, no sleeps

				wrappers[idx] = findBackendStore(udmrepo.StorageTypeS3)
			}(i)
		}

		done.Wait()

		seenWrappers := make(map[*kopiaBackendStore]bool, concurrentStoreCount)
		seenStores := make(map[backend.Store]bool, concurrentStoreCount)
		for _, w := range wrappers {
			require.NotNil(t, w)

			assert.False(t, seenWrappers[w],
				"findBackendStore returned a duplicate kopiaBackendStore wrapper to concurrent callers")
			seenWrappers[w] = true

			require.NotNil(t, w.store)
			assert.False(t, seenStores[w.store],
				"findBackendStore returned a duplicate backend.Store instance to concurrent callers")
			seenStores[w.store] = true
		}

		assert.Len(t, seenWrappers, concurrentStoreCount)
		assert.Len(t, seenStores, concurrentStoreCount)
	})

	t.Run("keeps per-instance setup state isolated under concurrent setup calls", func(t *testing.T) {
		restore := replaceBackendStoresWithCountingFactory()
		defer restore()

		logger := velerotest.NewLogger()
		ctx := t.Context()

		markers := make([]string, concurrentStoreCount)
		stores := make([]*kopiaBackendStore, concurrentStoreCount)
		setupErrs := make([]error, concurrentStoreCount)

		var start, done sync.WaitGroup
		start.Add(concurrentStoreCount)
		done.Add(concurrentStoreCount)

		for i := 0; i < concurrentStoreCount; i++ {
			marker := fmt.Sprintf("goroutine-%d", i)
			markers[i] = marker

			go func(idx int, mk string) {
				defer done.Done()
				start.Done()
				start.Wait() // release all goroutines together, no sleeps

				backendStore, err := setupBackendStore(ctx, udmrepo.StorageTypeS3, map[string]string{concurrentMarkerKey: mk}, logger)
				stores[idx] = backendStore
				setupErrs[idx] = err
			}(i, marker)
		}

		done.Wait()

		seenStores := make(map[backend.Store]bool, concurrentStoreCount)
		for i, s := range stores {
			require.NoError(t, setupErrs[i])
			require.NotNil(t, s)
			require.NotNil(t, s.store)

			assert.False(t, seenStores[s.store],
				"setupBackendStore reused a backend.Store instance for concurrent callers")
			seenStores[s.store] = true

			counting, ok := s.store.(*countingBackendStore)
			require.True(t, ok)

			assert.Equal(t, 1, counting.setupCount,
				"each store instance must be Setup exactly once, by its own acquirer only")
			assert.Equal(t, markers[i], counting.setupMarker,
				"each store instance must only observe the storage options passed by its own caller")
		}

		assert.Len(t, seenStores, concurrentStoreCount)
	})
}
