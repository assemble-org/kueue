/*
Copyright The Kubernetes Authors.

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

package tas

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
)

func TestPodsForPodSetSkipsElasticGatedMembers(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	member := func(name string) *testingpod.PodWrapper {
		return testingpod.MakePod(name, "ns").
			Label(constants.PodSetLabel, "role-a").
			Annotation(kueue.WorkloadSliceNameAnnotation, "grp").
			TopologySchedulingGate()
	}
	released := member("grp-0").Obj()
	heldBack := member("grp-1").Gate(kueue.ElasticJobSchedulingGate).Obj()
	kClient := utiltesting.NewClientBuilder().
		WithIndex(&corev1.Pod{}, coreindexer.WorkloadSliceNameKey, coreindexer.IndexPodWorkloadSliceName).
		WithObjects(released, heldBack).
		Build()
	ungater := newTopologyUngater(kClient, nil, nil)
	psa := &kueue.PodSetAssignment{
		Name:               "role-a",
		TopologyAssignment: &kueue.TopologyAssignment{Levels: []string{corev1.LabelHostname}},
	}

	pods, err := ungater.podsForPodSet(ctx, "ns", "grp", psa)
	if err != nil {
		t.Fatalf("podsForPodSet() error = %v", err)
	}
	got := make([]string, len(pods))
	for i, pod := range pods {
		got[i] = pod.Name
	}
	if diff := cmp.Diff([]string{"grp-0"}, got); diff != "" {
		t.Errorf("pods mismatch (-want,+got):\n%s", diff)
	}
}
