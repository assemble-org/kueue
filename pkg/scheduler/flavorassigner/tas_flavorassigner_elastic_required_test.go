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

package flavorassigner

import (
	"errors"
	"testing"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestWorkloadsTopologyRequests_ElasticRequiredTopology(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	tasFlavor := &schdcache.TASFlavorSnapshot{}
	cq := schdcache.ClusterQueueSnapshot{
		TASFlavors: map[kueue.ResourceFlavorReference]*schdcache.TASFlavorSnapshot{"tas": tasFlavor},
	}
	predecessor := func(unhealthyNodes ...string) *workload.Info {
		wl := &kueue.Workload{
			Status: kueue.WorkloadStatus{
				Admission: &kueue.Admission{
					PodSetAssignments: []kueue.PodSetAssignment{{
						Name:               kueue.DefaultPodSetName,
						TopologyAssignment: &kueue.TopologyAssignment{},
					}},
				},
			},
		}
		for _, node := range unhealthyNodes {
			wl.Status.UnhealthyNodes = append(wl.Status.UnhealthyNodes, kueue.UnhealthyNode{Name: node})
		}
		return workload.NewInfo(log, wl)
	}
	elasticRequired := func() *workload.Info {
		return workload.NewInfo(log, &kueue.Workload{
			Annotations: map[string]string{"kueue.x-k8s.io/elastic-job": "true"},
			Spec: kueue.WorkloadSpec{
				PodSets: []kueue.PodSet{
					*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 2).
						Request(corev1.ResourceCPU, "1").
						RequiredTopologyRequest(corev1.LabelHostname).
						Obj(),
				},
			},
		})
	}
	newAssignment := func(replaced *workload.Info) Assignment {
		return Assignment{
			PodSets: []PodSetAssignment{{
				Name:    kueue.DefaultPodSetName,
				Flavors: ResourceAssignment{corev1.ResourceCPU: {Name: "tas", Mode: Fit}},
				Count:   2,
				Status:  *NewStatus(),
			}},
			representativeMode:   new(Fit),
			replaceWorkloadSlice: replaced,
		}
	}

	cases := map[string]struct {
		requiredTopologyGate bool
		replaced             *workload.Info
		wantErr              error
		wantRequests         int
	}{
		"required topology is accepted behind the gate": {
			requiredTopologyGate: true,
			replaced:             predecessor(),
			wantRequests:         1,
		},
		"required topology is still rejected without the gate": {
			replaced: predecessor(),
			wantErr:  ErrElasticRequiredTopologyNotSupported,
		},
		"a replaced slice with an unhealthy node holds the scale-up": {
			requiredTopologyGate: true,
			replaced:             predecessor("x1"),
			wantErr:              ErrElasticPredecessorHasUnhealthyNodes,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesWithTAS, true)
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesWithRequiredTopology, tc.requiredTopologyGate)
			assignment := newAssignment(tc.replaced)
			wl := elasticRequired()

			tasReqs := assignment.WorkloadsTopologyRequests(testr.New(t), wl, &cq)
			got := assignment.PodSets[0].Status.err
			if tc.wantErr != nil {
				if !errors.Is(got, tc.wantErr) {
					t.Fatalf("got error %v, want %v", got, tc.wantErr)
				}
				if len(tasReqs) != 0 {
					t.Errorf("expected no TAS requests, got %+v", tasReqs)
				}
				return
			}
			if got != nil {
				t.Fatalf("unexpected error: %v", got)
			}
			if n := len(tasReqs["tas"]); n != tc.wantRequests {
				t.Errorf("got %d TAS requests for flavor tas, want %d", n, tc.wantRequests)
			}
		})
	}
}
