/*
Copyright the Velero Contributors.

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

package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func TestPatchDynamicPVDefaultSCGuard(t *testing.T) {
	wffc := storagev1api.VolumeBindingWaitForFirstConsumer
	immediate := storagev1api.VolumeBindingImmediate

	newCtx := func(t *testing.T) *finalizerContext {
		fakeClient := velerotest.NewFakeControllerRuntimeClient(t)
		volumeInfo := []*volume.BackupVolumeInfo{{
			BackupMethod: "PodVolumeBackup",
			PVCName:      "pvc1",
			PVName:       "pv1",
			PVCNamespace: "ns1",
			PVInfo: &volume.PVInfo{
				ReclaimPolicy: "Delete",
				Labels:        map[string]string{"l1": "v1"},
			},
		}}
		return &finalizerContext{
			logger:            velerotest.NewLogger(),
			crClient:          fakeClient,
			restore:           builder.ForRestore(velerov1api.DefaultNamespace, "restore").Result(),
			restoredPVCList:   map[string]struct{}{"ns1/pvc1": {}},
			backupVolumeInfos: volumeInfo,
			resourceTimeout:   2 * time.Second,
		}
	}

	createStorageClass := func(t *testing.T, ctx *finalizerContext, name string, bindingMode *storagev1api.VolumeBindingMode, annotations map[string]string) {
		sc := builder.ForStorageClass(name).Provisioner("prov").Result()
		sc.VolumeBindingMode = bindingMode
		sc.Annotations = annotations
		require.NoError(t, ctx.crClient.Create(t.Context(), sc))
	}

	t.Run("nil StorageClassName with WFFC default SC skips", func(t *testing.T) {
		ctx := newCtx(t)
		// nil means the PVC uses the cluster default StorageClass.
		createStorageClass(t, ctx, "default-sc", &wffc, map[string]string{"storageclass.kubernetes.io/is-default-class": "true"})

		pvc := builder.ForPersistentVolumeClaim("ns1", "pvc1").
			Phase(corev1api.ClaimPending).
			Result()
		require.Nil(t, pvc.Spec.StorageClassName)
		require.NoError(t, ctx.crClient.Create(t.Context(), pvc))

		start := time.Now()
		errs := ctx.patchDynamicPVWithVolumeInfo()
		elapsed := time.Since(start)

		require.Empty(t, errs.Namespaces)
		t.Logf("guard fired via default SC resolution: consumed %v of the %v resourceTimeout", elapsed, ctx.resourceTimeout)
	})

	t.Run("empty StorageClassName does not consume the default SC", func(t *testing.T) {
		// An explicit spec.storageClassName: "" means "no StorageClass" —
		// it must NOT trigger the default-SC WFFC skip.
		ctx := newCtx(t)
		createStorageClass(t, ctx, "default-sc", &wffc, map[string]string{"storageclass.kubernetes.io/is-default-class": "true"})

		pvc := builder.ForPersistentVolumeClaim("ns1", "pvc1").
			Phase(corev1api.ClaimPending).
			Result()
		empty := ""
		pvc.Spec.StorageClassName = &empty
		require.NoError(t, ctx.crClient.Create(t.Context(), pvc))

		errs := ctx.patchDynamicPVWithVolumeInfo()

		// The PVC has no StorageClass so the WFFC guard does not fire; the
		// poll runs until resourceTimeout and reports the error.
		require.NotEmpty(t, errs.Namespaces, "empty storageClassName must NOT resolve the default SC — the WFFC skip must not fire")
	})

	t.Run("nil StorageClassName with multiple default SCs, any WFFC skips", func(t *testing.T) {
		ctx := newCtx(t)
		// Two SCs annotated as default during a transition; one has WFFC.
		createStorageClass(t, ctx, "default-old", &immediate, map[string]string{"storageclass.kubernetes.io/is-default-class": "true"})
		createStorageClass(t, ctx, "default-new", &wffc, map[string]string{"storageclass.kubernetes.io/is-default-class": "true"})

		pvc := builder.ForPersistentVolumeClaim("ns1", "pvc1").
			Phase(corev1api.ClaimPending).
			Result()
		require.Nil(t, pvc.Spec.StorageClassName)
		require.NoError(t, ctx.crClient.Create(t.Context(), pvc))

		errs := ctx.patchDynamicPVWithVolumeInfo()
		require.Empty(t, errs.Namespaces, "any WFFC default SC must trigger the skip")
	})

	t.Run("explicit WFFC SC skips", func(t *testing.T) {
		ctx := newCtx(t)
		createStorageClass(t, ctx, "sc-wffc", &wffc, nil)

		pvc := builder.ForPersistentVolumeClaim("ns1", "pvc1").
			StorageClass("sc-wffc").
			Phase(corev1api.ClaimPending).
			Result()
		require.NoError(t, ctx.crClient.Create(t.Context(), pvc))

		errs := ctx.patchDynamicPVWithVolumeInfo()
		require.Empty(t, errs.Namespaces)
	})

	t.Run("explicit Immediate SC proceeds to patch", func(t *testing.T) {
		ctx := newCtx(t)
		createStorageClass(t, ctx, "sc-immediate", &immediate, nil)

		pvc := builder.ForPersistentVolumeClaim("ns1", "pvc1").
			StorageClass("sc-immediate").
			VolumeName("new-pv1").
			Phase(corev1api.ClaimBound).
			Result()
		require.NoError(t, ctx.crClient.Create(t.Context(), pvc))

		pv := builder.ForPersistentVolume("new-pv1").
			ClaimRef("ns1", "pvc1").
			Phase(corev1api.VolumeBound).
			ReclaimPolicy(corev1api.PersistentVolumeReclaimRetain).
			Result()
		require.NoError(t, ctx.crClient.Create(t.Context(), pv))

		errs := ctx.patchDynamicPVWithVolumeInfo()
		require.Empty(t, errs.Namespaces)

		got := &corev1api.PersistentVolume{}
		require.NoError(t, ctx.crClient.Get(t.Context(), types.NamespacedName{Name: "new-pv1"}, got))
		assert.Equal(t, corev1api.PersistentVolumeReclaimDelete, got.Spec.PersistentVolumeReclaimPolicy,
			"the PV reclaim policy must be patched from Retain to Delete for the Immediate SC")
	})
}
