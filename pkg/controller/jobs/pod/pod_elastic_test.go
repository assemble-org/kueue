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

package pod

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

const (
	elasticTestNamespace = "test-ns"
	elasticTestGroup     = "grp"
	// Role hash of the testingpod.MakePod spec; labels and annotations do not change it.
	elasticTestRoleHash = "a9f06f3a"
)

var elasticGroupGates = map[featuregate.Feature]bool{
	features.TopologyAwareScheduling:                  false,
	features.ElasticJobsViaWorkloadSlices:             true,
	features.ElasticJobsViaWorkloadSlicesForPodGroups: true,
}

// elasticGroupMember is a pod of the group: 4 members, minimum 2, opted into slicing.
func elasticGroupMember(name string) *testingpod.PodWrapper {
	return testingpod.MakePod(name, elasticTestNamespace).
		Queue("test-queue").
		GroupNameLabel(elasticTestGroup).
		GroupTotalCount("4").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Annotation(podconstants.GroupMinCountAnnotation, "2")
}

func newElasticTestWebhook(t *testing.T, objects ...interface{ GetName() string }) *PodWebhook {
	t.Helper()
	namespace := utiltesting.MakeNamespaceWrapper(elasticTestNamespace).Label(corev1.LabelMetadataName, elasticTestNamespace).Obj()
	cli := utiltesting.NewClientBuilder().WithObjects(namespace).Build()
	integrationManager := newTestIntegrationManager(t)
	t.Cleanup(integrationManager.EnableIntegrationsForTest(t))
	return &PodWebhook{
		integrationManager: integrationManager,
		client:             cli,
		queues:             qcache.NewManagerForUnitTests(cli, schdcache.New(cli)),
	}
}

func TestDefaultElasticGroupMember(t *testing.T) {
	testCases := map[string]struct {
		featureGates map[featuregate.Feature]bool
		want         *corev1.Pod
	}{
		"elastic group member gets the podset label, the slice name and the elastic gate": {
			featureGates: elasticGroupGates,
			want: elasticGroupMember("grp-0").
				ManagedByKueueLabel().
				KueueSchedulingGate().
				Gate(kueue.ElasticJobSchedulingGate).
				RoleHash(elasticTestRoleHash).
				Label(kueueconstants.PodSetLabel, elasticTestRoleHash).
				Annotation(kueue.WorkloadSliceNameAnnotation, elasticTestGroup).
				KueueFinalizer().
				Obj(),
		},
		"pod groups gate off leaves a plain group member": {
			featureGates: map[featuregate.Feature]bool{
				features.TopologyAwareScheduling:                  false,
				features.ElasticJobsViaWorkloadSlices:             true,
				features.ElasticJobsViaWorkloadSlicesForPodGroups: false,
			},
			want: elasticGroupMember("grp-0").
				ManagedByKueueLabel().
				KueueSchedulingGate().
				RoleHash(elasticTestRoleHash).
				KueueFinalizer().
				Obj(),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)
			w := newElasticTestWebhook(t)
			ctx, _ := utiltesting.ContextWithLog(t)

			pod := elasticGroupMember("grp-0").Obj()
			if err := w.Default(ctx, pod); err != nil {
				t.Fatalf("Default() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, pod); diff != "" {
				t.Errorf("Pod mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestValidateCreateElasticGroup(t *testing.T) {
	elasticPath := "metadata.annotations[kueue.x-k8s.io/elastic-job]"
	minCountPath := "metadata.annotations[kueue.x-k8s.io/pod-group-min-count]"
	roleCountPath := "metadata.annotations[kueue.x-k8s.io/pod-group-role-count]"
	testCases := map[string]struct {
		featureGates map[featuregate.Feature]bool
		pod          *corev1.Pod
		wantErr      error
	}{
		"elastic group with a valid minimum is accepted": {
			pod: elasticGroupMember("grp-0").Obj(),
		},
		"elastic annotation without a group is forbidden": {
			pod: testingpod.MakePod("grp-0", elasticTestNamespace).
				Queue("test-queue").
				Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
				Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeForbidden, Field: elasticPath}}.ToAggregate(),
		},
		"minimum without the elastic annotation is forbidden": {
			pod: testingpod.MakePod("grp-0", elasticTestNamespace).
				Queue("test-queue").
				GroupNameLabel(elasticTestGroup).
				GroupTotalCount("4").
				Annotation(podconstants.GroupMinCountAnnotation, "2").
				Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeForbidden, Field: minCountPath}}.ToAggregate(),
		},
		"minimum above the total count is invalid": {
			pod:     elasticGroupMember("grp-0").Annotation(podconstants.GroupMinCountAnnotation, "5").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeInvalid, Field: minCountPath}}.ToAggregate(),
		},
		"zero minimum is invalid": {
			pod:     elasticGroupMember("grp-0").Annotation(podconstants.GroupMinCountAnnotation, "0").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeInvalid, Field: minCountPath}}.ToAggregate(),
		},
		"role count within the total count is accepted": {
			pod: elasticGroupMember("grp-0").Annotation(podconstants.GroupRoleCountAnnotation, "2").Obj(),
		},
		"role count without the elastic annotation is forbidden": {
			pod: testingpod.MakePod("grp-0", elasticTestNamespace).
				Queue("test-queue").
				GroupNameLabel(elasticTestGroup).
				GroupTotalCount("4").
				Annotation(podconstants.GroupRoleCountAnnotation, "2").
				Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeForbidden, Field: roleCountPath}}.ToAggregate(),
		},
		"role count above the total count is invalid": {
			pod:     elasticGroupMember("grp-0").Annotation(podconstants.GroupRoleCountAnnotation, "5").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeInvalid, Field: roleCountPath}}.ToAggregate(),
		},
		"minimum above the role count is invalid": {
			pod:     elasticGroupMember("grp-0").Annotation(podconstants.GroupRoleCountAnnotation, "1").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeInvalid, Field: minCountPath}}.ToAggregate(),
		},
		"elastic pod is rejected while the pod groups gate is off": {
			featureGates: map[featuregate.Feature]bool{
				features.ElasticJobsViaWorkloadSlices:             true,
				features.ElasticJobsViaWorkloadSlicesForPodGroups: false,
			},
			pod:     elasticGroupMember("grp-0").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeForbidden, Field: elasticPath}}.ToAggregate(),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = elasticGroupGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			w := newElasticTestWebhook(t)
			ctx, _ := utiltesting.ContextWithLog(t)

			_, err := w.ValidateCreate(ctx, tc.pod)
			if diff := cmp.Diff(tc.wantErr, err, cmpopts.IgnoreFields(field.Error{}, "BadValue", "Detail")); diff != "" {
				t.Errorf("Unexpected error (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestValidateUpdateElasticGroupMinCount(t *testing.T) {
	minCountPath := "metadata.annotations[kueue.x-k8s.io/pod-group-min-count]"
	testCases := map[string]struct {
		newPod  *corev1.Pod
		wantErr error
	}{
		"unchanged minimum is accepted": {
			newPod: elasticGroupMember("grp-0").Obj(),
		},
		"changed minimum is immutable": {
			newPod:  elasticGroupMember("grp-0").Annotation(podconstants.GroupMinCountAnnotation, "3").Obj(),
			wantErr: field.ErrorList{&field.Error{Type: field.ErrorTypeInvalid, Field: minCountPath}}.ToAggregate(),
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, elasticGroupGates)
			w := newElasticTestWebhook(t)
			ctx, _ := utiltesting.ContextWithLog(t)

			_, err := w.ValidateUpdate(ctx, elasticGroupMember("grp-0").Obj(), tc.newPod)
			if diff := cmp.Diff(tc.wantErr, err, cmpopts.IgnoreFields(field.Error{}, "BadValue", "Detail")); diff != "" {
				t.Errorf("Unexpected error (-want,+got):\n%s", diff)
			}
		})
	}
}

// elasticGroup builds the reconciled group from its members; the first member is the reconciled pod.
func elasticGroup(members ...*corev1.Pod) *Pod {
	items := make([]corev1.Pod, len(members))
	for i := range members {
		items[i] = *members[i]
	}
	return &Pod{
		pod:     items[0],
		isFound: true,
		isGroup: true,
		list:    corev1.PodList{Items: items},
	}
}

func TestConstructGroupPodSetsElasticGroup(t *testing.T) {
	members := func(count int, minCount string, roleHashes ...string) []*corev1.Pod {
		pods := make([]*corev1.Pod, count)
		for i := range pods {
			member := elasticGroupMember(fmt.Sprintf("grp-%d", i)).RoleHash(roleHashes[i%len(roleHashes)])
			if minCount == "" {
				delete(member.Annotations, podconstants.GroupMinCountAnnotation)
			} else {
				member.Annotation(podconstants.GroupMinCountAnnotation, minCount)
			}
			pods[i] = member.Obj()
		}
		return pods
	}
	testCases := map[string]struct {
		featureGates map[featuregate.Feature]bool
		members      []*corev1.Pod
		wantCount    int32
		wantMinCount *int32
		wantErr      bool
	}{
		"slice is sized by the total count with the minimum as floor": {
			members:      members(3, "2", "role-a"),
			wantCount:    4,
			wantMinCount: ptr.To[int32](2),
		},
		"minimum above the total count is capped": {
			members:      members(4, "6", "role-a"),
			wantCount:    4,
			wantMinCount: ptr.To[int32](4),
		},
		"no minimum leaves the floor unset": {
			members:   members(4, "", "role-a"),
			wantCount: 4,
		},
		"two roles without role counts are rejected": {
			members: members(4, "2", "role-a", "role-b"),
			wantErr: true,
		},
		"pod groups gate off keeps the live member count": {
			featureGates: map[featuregate.Feature]bool{
				features.ElasticJobsViaWorkloadSlices:             true,
				features.ElasticJobsViaWorkloadSlicesForPodGroups: false,
			},
			members:   members(3, "2", "role-a"),
			wantCount: 3,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = elasticGroupGates
			}
			features.SetFeatureGatesDuringTest(t, gates)

			podSets, err := elasticGroup(tc.members...).constructGroupPodSets(nil)
			if tc.wantErr {
				if !jobframework.IsUnretryableError(err) {
					t.Fatalf("constructGroupPodSets() error = %v, want an unretryable error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("constructGroupPodSets() error = %v", err)
			}
			if len(podSets) != 1 {
				t.Fatalf("got %d PodSets, want 1", len(podSets))
			}
			if podSets[0].Count != tc.wantCount {
				t.Errorf("Count = %d, want %d", podSets[0].Count, tc.wantCount)
			}
			if diff := cmp.Diff(tc.wantMinCount, podSets[0].MinCount); diff != "" {
				t.Errorf("MinCount mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestConstructGroupPodSetsElasticTemplate(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, elasticGroupGates)
	// A released member carries the node selectors the ungaters injected.
	released := func(name, node string) *corev1.Pod {
		return elasticGroupMember(name).RoleHash("role-a").
			NodeSelector("kubernetes.io/hostname", node).
			NodeSelector("example.com/pool", "gpu").
			StatusPhase(corev1.PodRunning).Obj()
	}
	gated := func(name string) *corev1.Pod {
		return elasticGroupMember(name).RoleHash("role-a").Gate(kueue.ElasticJobSchedulingGate).Obj()
	}
	testCases := map[string]struct {
		members          []*corev1.Pod
		wantNodeSelector map[string]string
	}{
		"the template comes from a member still holding the elastic gate": {
			members: []*corev1.Pod{released("grp-0", "node-a"), released("grp-1", "node-b"), gated("grp-2"), gated("grp-3")},
		},
		"with every member released the first one is the template": {
			members:          []*corev1.Pod{released("grp-0", "node-a"), released("grp-1", "node-b")},
			wantNodeSelector: map[string]string{"kubernetes.io/hostname": "node-a", "example.com/pool": "gpu"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			podSets, err := elasticGroup(tc.members...).constructGroupPodSets(nil)
			if err != nil {
				t.Fatalf("constructGroupPodSets() error = %v", err)
			}
			if len(podSets) != 1 {
				t.Fatalf("got %d PodSets, want 1", len(podSets))
			}
			if diff := cmp.Diff(tc.wantNodeSelector, podSets[0].Template.Spec.NodeSelector); diff != "" {
				t.Errorf("template nodeSelector mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestConstructGroupPodSetsElasticRoles(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, elasticGroupGates)
	// member is a pod of an elastic group of 4 with the given role, role count and
	// minimum; a released member carries the node selector the ungaters injected.
	member := func(name, role, roleCount, minCount string, gated bool) *corev1.Pod {
		w := testingpod.MakePod(name, elasticTestNamespace).
			Queue("test-queue").
			GroupNameLabel(elasticTestGroup).
			GroupTotalCount("4").
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			RoleHash(role)
		if roleCount != "" {
			w.Annotation(podconstants.GroupRoleCountAnnotation, roleCount)
		}
		if minCount != "" {
			w.Annotation(podconstants.GroupMinCountAnnotation, minCount)
		}
		if gated {
			w.Gate(kueue.ElasticJobSchedulingGate)
		} else {
			w.NodeSelector("kubernetes.io/hostname", "node-"+name).StatusPhase(corev1.PodRunning)
		}
		return w.Obj()
	}
	type podSetShape struct {
		Name     string
		Count    int32
		MinCount *int32
	}
	testCases := map[string]struct {
		members []*corev1.Pod
		want    []podSetShape
		wantErr bool
	}{
		"one PodSet per role, sized by the role count, with the role's own minimum": {
			members: []*corev1.Pod{
				member("grp-0", "role-a", "2", "1", false),
				member("grp-1", "role-a", "2", "1", true),
				member("grp-2", "role-b", "2", "2", true),
				member("grp-3", "role-b", "2", "2", true),
			},
			want: []podSetShape{{"role-a", 2, ptr.To[int32](1)}, {"role-b", 2, ptr.To[int32](2)}},
		},
		"a second role without a role count is rejected": {
			members: []*corev1.Pod{
				member("grp-0", "role-a", "", "2", true),
				member("grp-1", "role-b", "", "2", true),
			},
			wantErr: true,
		},
		"role counts that do not add up to the total count are rejected": {
			members: []*corev1.Pod{
				member("grp-0", "role-a", "2", "", true),
				member("grp-1", "role-b", "3", "", true),
			},
			wantErr: true,
		},
		"a single role is still sized by the group total count": {
			members: []*corev1.Pod{
				member("grp-0", "role-a", "2", "2", true),
				member("grp-1", "role-a", "2", "2", true),
			},
			want: []podSetShape{{"role-a", 4, ptr.To[int32](2)}},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			podSets, err := elasticGroup(tc.members...).constructGroupPodSets(nil)
			if tc.wantErr {
				if !jobframework.IsUnretryableError(err) {
					t.Fatalf("constructGroupPodSets() error = %v, want an unretryable error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("constructGroupPodSets() error = %v", err)
			}
			got := make([]podSetShape, 0, len(podSets))
			for _, ps := range podSets {
				got = append(got, podSetShape{string(ps.Name), ps.Count, ps.MinCount})
				if ps.Template.Spec.NodeSelector != nil {
					t.Errorf("PodSet %s copied a released member's nodeSelector %v into its template", ps.Name, ps.Template.Spec.NodeSelector)
				}
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("PodSets mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPodsReadyElasticGroup(t *testing.T) {
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	readyMembers := func() []*corev1.Pod {
		return []*corev1.Pod{
			elasticGroupMember("grp-0").StatusConditions(ready).Obj(),
			elasticGroupMember("grp-1").StatusConditions(ready).Obj(),
			elasticGroupMember("grp-2").StatusConditions(ready).Obj(),
		}
	}
	testCases := map[string]struct {
		featureGates map[featuregate.Feature]bool
		fourth       *corev1.Pod
		want         bool
	}{
		"a member still holding the elastic gate is not expected to be ready": {
			fourth: elasticGroupMember("grp-3").Gate(kueue.ElasticJobSchedulingGate).Obj(),
			want:   true,
		},
		"a released member that is not ready keeps the group not ready": {
			fourth: elasticGroupMember("grp-3").Obj(),
			want:   false,
		},
		"pod groups gate off counts every member": {
			featureGates: map[featuregate.Feature]bool{
				features.ElasticJobsViaWorkloadSlices:             true,
				features.ElasticJobsViaWorkloadSlicesForPodGroups: false,
			},
			fourth: elasticGroupMember("grp-3").Gate(kueue.ElasticJobSchedulingGate).Obj(),
			want:   false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = elasticGroupGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			ctx, _ := utiltesting.ContextWithLog(t)

			group := elasticGroup(append(readyMembers(), tc.fourth)...)
			if got := group.PodsReady(ctx, nil); got != tc.want {
				t.Errorf("PodsReady() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("nothing released yet keeps the group not ready", func(t *testing.T) {
		features.SetFeatureGatesDuringTest(t, elasticGroupGates)
		ctx, _ := utiltesting.ContextWithLog(t)
		var members []*corev1.Pod
		for i := range 4 {
			members = append(members, elasticGroupMember(fmt.Sprintf("grp-%d", i)).Gate(kueue.ElasticJobSchedulingGate).Obj())
		}
		if elasticGroup(members...).PodsReady(ctx, nil) {
			t.Error("PodsReady() = true for a group whose members are all still elastic-gated, want false")
		}
	})
}

func TestPrepareNeverAddsSchedulingGates(t *testing.T) {
	tasInfo := podset.PodSetInfo{Name: "role-a", SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}}}
	plainInfo := podset.PodSetInfo{Name: "role-a"}
	testCases := map[string]struct {
		pod       *corev1.Pod
		info      podset.PodSetInfo
		wantGates []corev1.PodSchedulingGate
	}{
		"a topology gate the ungater already removed is not re-added": {
			pod:  testingpod.MakePod("p", "ns").KueueSchedulingGate().Obj(),
			info: tasInfo,
		},
		"the topology gate is kept while the admission uses TAS": {
			pod:       testingpod.MakePod("p", "ns").KueueSchedulingGate().TopologySchedulingGate().Obj(),
			info:      tasInfo,
			wantGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
		"the topology gate is removed when the admission does not use TAS": {
			pod:  testingpod.MakePod("p", "ns").KueueSchedulingGate().TopologySchedulingGate().Obj(),
			info: plainInfo,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			if err := prepare(log, tc.pod, tc.info); err != nil {
				t.Fatalf("prepare() error = %v", err)
			}
			if diff := cmp.Diff(tc.wantGates, tc.pod.Spec.SchedulingGates, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("scheduling gates mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestWorkloadSliceNameElasticGroup(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, elasticGroupGates)
	if got := elasticGroup(elasticGroupMember("grp-0").Obj()).WorkloadSliceName(); got != elasticTestGroup {
		t.Errorf("WorkloadSliceName() = %q, want %q", got, elasticTestGroup)
	}
	plain := testingpod.MakePod("grp-0", elasticTestNamespace).GroupNameLabel(elasticTestGroup).GroupTotalCount("4").Obj()
	if got := elasticGroup(plain).WorkloadSliceName(); got != "" {
		t.Errorf("WorkloadSliceName() = %q for a plain group, want empty", got)
	}
}
