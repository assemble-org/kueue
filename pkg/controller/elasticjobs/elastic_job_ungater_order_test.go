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

package elasticjobs

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
)

func TestCompareUngateCandidates(t *testing.T) {
	testCases := map[string]struct {
		pods      []*corev1.Pod
		wantOrder []string
	}{
		"indices order numerically, not lexicographically": {
			pods: []*corev1.Pod{
				testingpod.MakePod("grp-10", "ns").GroupIndex("10").Obj(),
				testingpod.MakePod("grp-2", "ns").GroupIndex("2").Obj(),
				testingpod.MakePod("grp-1", "ns").GroupIndex("1").Obj(),
			},
			wantOrder: []string{"grp-1", "grp-2", "grp-10"},
		},
		"pods without an index order by name": {
			pods: []*corev1.Pod{
				testingpod.MakePod("b", "ns").Obj(),
				testingpod.MakePod("a", "ns").Obj(),
			},
			wantOrder: []string{"a", "b"},
		},
		"a pod without an index orders by name against an indexed one": {
			pods: []*corev1.Pod{
				testingpod.MakePod("z", "ns").GroupIndex("0").Obj(),
				testingpod.MakePod("a", "ns").Obj(),
			},
			wantOrder: []string{"a", "z"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			slices.SortFunc(tc.pods, compareUngateCandidates)
			got := make([]string, len(tc.pods))
			for i, pod := range tc.pods {
				got[i] = pod.Name
			}
			if diff := cmp.Diff(tc.wantOrder, got); diff != "" {
				t.Errorf("order mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}
