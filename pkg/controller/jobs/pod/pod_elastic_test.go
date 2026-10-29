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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	"sigs.k8s.io/kueue/pkg/util/expectations"
	"sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
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

// TestCleanupMembersElasticGroup covers the member cleanup the reconciler runs on
// the workload-slice path, where a group never reaches FindMatchingWorkloads.
func TestCleanupMembersElasticGroup(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	slice := utiltestingapi.MakeWorkload(elasticTestGroup, elasticTestNamespace).Group().
		PodSets(*utiltestingapi.MakePodSet(kueue.NewPodSetReference(elasticTestRoleHash), 4).Obj()).
		Obj()
	running := func(name string, age time.Duration) *corev1.Pod {
		return elasticGroupMember(name).UID(name).KueueFinalizer().
			CreationTimestamp(now.Add(-age)).StatusPhase(corev1.PodRunning).Obj()
	}
	// A member that failed and was deleted: Kueue's finalizer is all that keeps it.
	failed := func(name string) *corev1.Pod {
		return elasticGroupMember(name).UID(name).KueueFinalizer().
			CreationTimestamp(now.Add(-time.Hour)).StatusPhase(corev1.PodFailed).
			DeletionTimestamp(now).Obj()
	}
	threeRunning := func() []*corev1.Pod {
		return []*corev1.Pod{
			running("grp-0", time.Hour),
			running("grp-1", time.Hour),
			running("grp-2", time.Hour),
		}
	}
	testCases := map[string]struct {
		pods       []*corev1.Pod
		pendingOps bool
		wantErr    error
		wantGone   []string
		wantKept   []string
		wantAbsent int
	}{
		"the failed member its replacement stands in for is finalized": {
			pods:     append(threeRunning(), failed("grp-3"), running("grp-3-r", time.Minute)),
			wantGone: []string{"grp-3"},
			wantKept: []string{"grp-0", "grp-1", "grp-2", "grp-3-r"},
		},
		"the youngest active member beyond the role count is deleted": {
			pods:     append(threeRunning(), running("grp-3", time.Hour), running("grp-4", time.Minute)),
			wantGone: []string{"grp-4"},
			wantKept: []string{"grp-0", "grp-1", "grp-2", "grp-3"},
		},
		"a failed member stays until something replaces it": {
			pods:       append(threeRunning(), failed("grp-3")),
			wantKept:   []string{"grp-0", "grp-1", "grp-2", "grp-3"},
			wantAbsent: 1,
		},
		"nothing is released before earlier operations on pods are observed": {
			pods:       append(threeRunning(), failed("grp-3"), running("grp-3-r", time.Minute)),
			pendingOps: true,
			wantErr:    errPendingOps,
			wantKept:   []string{"grp-0", "grp-1", "grp-2", "grp-3", "grp-3-r"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, elasticGroupGates)
			ctx, _ := utiltesting.ContextWithLog(t)
			objects := make([]client.Object, 0, len(tc.pods))
			for _, pod := range tc.pods {
				objects = append(objects, pod.DeepCopy())
			}
			cli := utiltesting.NewClientBuilder().WithObjects(objects...).Build()

			group := elasticGroup(tc.pods...)
			group.key = types.NamespacedName{Namespace: elasticTestNamespace, Name: elasticTestGroup}
			group.excessPodExpectations = expectations.NewStore("test")
			group.satisfiedExcessPods = !tc.pendingOps
			group.clock = realClock
			recorder := &utiltesting.EventRecorder{}

			err := group.CleanupMembers(ctx, cli, recorder, slice)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CleanupMembers() error = %v, want %v", err, tc.wantErr)
			}
			for _, pod := range tc.pods {
				var got corev1.Pod
				getErr := cli.Get(ctx, client.ObjectKeyFromObject(pod), &got)
				gone := slices.Contains(tc.wantGone, pod.Name)
				switch {
				case gone && !apierrors.IsNotFound(getErr):
					t.Errorf("pod %s: still exists (err=%v), want it released", pod.Name, getErr)
				case !gone && getErr != nil:
					t.Errorf("pod %s: get error %v, want it kept", pod.Name, getErr)
				case !gone && !slices.Contains(got.Finalizers, podconstants.PodFinalizer):
					t.Errorf("pod %s: finalizer removed, want it kept", pod.Name)
				}
			}
			var kept []string
			for i := range group.list.Items {
				kept = append(kept, group.list.Items[i].Name)
			}
			slices.Sort(kept)
			if diff := cmp.Diff(tc.wantKept, kept); diff != "" {
				t.Errorf("kept members (-want +got):\n%s", diff)
			}
			if group.absentPods != tc.wantAbsent {
				t.Errorf("absentPods = %d, want %d", group.absentPods, tc.wantAbsent)
			}
		})
	}
}

func TestCleanupMembersStrandedByDomainFailover(t *testing.T) {
	const rackLabel = "cloud.com/topology-rack"
	now := time.Now().Truncate(time.Second)
	gates := map[featuregate.Feature]bool{
		features.ElasticJobsViaWorkloadSlicesForPodGroups:   true,
		features.ElasticJobsViaWorkloadSlicesDomainFailover: true,
	}
	// The role is assigned to rack r2 (y1, y2, y3) after a failover away from
	// rack r1 (x1).
	slice := func(optIn bool) *kueue.Workload {
		wl := utiltestingapi.MakeWorkload(elasticTestGroup, elasticTestNamespace).Group().
			Annotation(kueueconstants.ElasticJobAnnotation, "true").
			PodSets(*utiltestingapi.MakePodSet(kueue.NewPodSetReference(elasticTestRoleHash), 3).
				RequiredTopologyRequest(rackLabel).Obj()).
			Admission(utiltestingapi.MakeAdmission("cq", kueue.NewPodSetReference(elasticTestRoleHash)).
				PodSets(utiltestingapi.MakePodSetAssignment(kueue.NewPodSetReference(elasticTestRoleHash)).
					Count(3).
					TopologyAssignment(utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
						Domain(tas.TopologyDomainAssignment{Count: 1, Values: []string{"y1"}}).
						Domain(tas.TopologyDomainAssignment{Count: 1, Values: []string{"y2"}}).
						Domain(tas.TopologyDomainAssignment{Count: 1, Values: []string{"y3"}}).
						Obj()).
					Obj()).
				Obj())
		if optIn {
			wl.Annotation(kueueconstants.ElasticJobDomainFailoverAnnotationKey, kueueconstants.ElasticJobDomainFailoverAnnotationValue)
		}
		return wl.Obj()
	}
	node := func(name, rack string) *corev1.Node {
		return testingnode.MakeNode(name).Label(rackLabel, rack).Label(corev1.LabelHostname, name).Obj()
	}
	nodes := []client.Object{node("x1", "r1"), node("y1", "r2"), node("y2", "r2"), node("y3", "r2")}
	// The webhook stamps the role hash before the ungater adds a node selector,
	// so a scheduled member carries it as an annotation.
	runningOn := func(name, nodeName string) *corev1.Pod {
		return elasticGroupMember(name).UID(name).KueueFinalizer().RoleHash(elasticTestRoleHash).
			CreationTimestamp(now.Add(-time.Hour)).NodeName(nodeName).
			NodeSelector(corev1.LabelHostname, nodeName).StatusPhase(corev1.PodRunning).Obj()
	}
	gated := func(name string) *corev1.Pod {
		return elasticGroupMember(name).UID(name).KueueFinalizer().RoleHash(elasticTestRoleHash).
			CreationTimestamp(now.Add(-time.Minute)).TopologySchedulingGate().StatusPhase(corev1.PodPending).Obj()
	}
	testCases := map[string]struct {
		workload   *kueue.Workload
		nodes      []client.Object
		pods       []*corev1.Pod
		pendingOps bool
		// reconcileAgain runs a second CleanupMembers with the expectations the
		// first one recorded, once the deleted member has terminated and its
		// replacement exists, as the next reconcile of the group finds them.
		reconcileAgain bool
		wantErr        error
		wantStranded   []string
		// wantFinalized are the pods the second reconcile released.
		wantFinalized []string
	}{
		"the member left in the previous rack is deleted; the member in the new rack and the gated member stay": {
			workload:     slice(true),
			nodes:        nodes,
			pods:         []*corev1.Pod{runningOn("grp-0", "x1"), runningOn("grp-1", "y1"), gated("grp-2")},
			wantStranded: []string{"grp-0"},
		},
		"a deleted stranded member does not hold up the next reconcile, which releases it to its replacement": {
			workload:       slice(true),
			nodes:          nodes,
			pods:           []*corev1.Pod{runningOn("grp-0", "x1"), runningOn("grp-1", "y1"), gated("grp-2")},
			reconcileAgain: true,
			wantStranded:   []string{"grp-0"},
			wantFinalized:  []string{"grp-0"},
		},
		"a member already terminating is left alone": {
			workload: slice(true),
			nodes:    nodes,
			pods: []*corev1.Pod{
				elasticGroupMember("grp-0").UID("grp-0").KueueFinalizer().RoleHash(elasticTestRoleHash).CreationTimestamp(now.Add(-time.Hour)).
					NodeName("x1").StatusPhase(corev1.PodRunning).DeletionTimestamp(now).Obj(),
				runningOn("grp-1", "y1"), gated("grp-2"),
			},
		},
		"a member whose node is gone is left to end with it": {
			workload: slice(true),
			nodes:    nodes[1:],
			pods:     []*corev1.Pod{runningOn("grp-0", "x1"), runningOn("grp-1", "y1"), gated("grp-2")},
		},
		"without the opt-in annotation nothing is deleted": {
			workload: slice(false),
			nodes:    nodes,
			pods:     []*corev1.Pod{runningOn("grp-0", "x1"), runningOn("grp-1", "y1"), gated("grp-2")},
		},
		"nothing is deleted before earlier operations on pods are observed": {
			workload:   slice(true),
			nodes:      nodes,
			pods:       []*corev1.Pod{runningOn("grp-0", "x1"), runningOn("grp-1", "y1"), gated("grp-2")},
			pendingOps: true,
			wantErr:    errPendingOps,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, gates)
			ctx, log := utiltesting.ContextWithLog(t)
			objects := slices.Clone(tc.nodes)
			for _, pod := range tc.pods {
				objects = append(objects, pod.DeepCopy())
			}
			cli := utiltesting.NewClientBuilder().WithObjects(objects...).Build()
			key := types.NamespacedName{Namespace: elasticTestNamespace, Name: elasticTestGroup}
			store := expectations.NewStore("test")

			group := elasticGroup(tc.pods...)
			group.key = key
			group.excessPodExpectations = store
			group.satisfiedExcessPods = !tc.pendingOps
			group.clock = realClock
			recorder := &utiltesting.EventRecorder{}

			err := group.CleanupMembers(ctx, cli, recorder, tc.workload)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CleanupMembers() error = %v, want %v", err, tc.wantErr)
			}
			if tc.reconcileAgain {
				// The deleted member ends its container with exit code 0 and its
				// replacement arrives before the group is reconciled again.
				for _, name := range tc.wantStranded {
					var deleted corev1.Pod
					if err := cli.Get(ctx, client.ObjectKey{Namespace: elasticTestNamespace, Name: name}, &deleted); err != nil {
						t.Fatalf("pod %s: get error %v", name, err)
					}
					deleted.Status.Phase = corev1.PodSucceeded
					if err := cli.Status().Update(ctx, &deleted); err != nil {
						t.Fatalf("pod %s: status update error %v", name, err)
					}
					if err := cli.Create(ctx, gated(name+"-r")); err != nil {
						t.Fatalf("pod %s-r: create error %v", name, err)
					}
				}
				var podList corev1.PodList
				if err := cli.List(ctx, &podList, client.InNamespace(elasticTestNamespace)); err != nil {
					t.Fatalf("listing pods: %v", err)
				}
				members := make([]*corev1.Pod, len(podList.Items))
				for i := range podList.Items {
					members[i] = &podList.Items[i]
				}
				again := elasticGroup(members...)
				again.key = key
				again.excessPodExpectations = store
				again.satisfiedExcessPods = store.Satisfied(log, key)
				again.clock = realClock
				if err := again.CleanupMembers(ctx, cli, recorder, tc.workload); err != nil {
					t.Fatalf("second CleanupMembers() error = %v, want nil", err)
				}
			}
			var gotStranded []string
			for _, pod := range tc.pods {
				var got corev1.Pod
				err := cli.Get(ctx, client.ObjectKeyFromObject(pod), &got)
				if slices.Contains(tc.wantFinalized, pod.Name) {
					if err == nil && slices.Contains(got.Finalizers, podconstants.PodFinalizer) {
						t.Errorf("pod %s: finalizer kept, want it released to the replacement", pod.Name)
					}
					if client.IgnoreNotFound(err) != nil {
						t.Fatalf("pod %s: get error %v", pod.Name, err)
					}
					gotStranded = append(gotStranded, pod.Name)
					continue
				}
				if err != nil {
					t.Fatalf("pod %s: get error %v, want it kept until replaced", pod.Name, err)
				}
				if !slices.Contains(got.Finalizers, podconstants.PodFinalizer) {
					t.Errorf("pod %s: finalizer removed, want it kept", pod.Name)
				}
				if !got.DeletionTimestamp.IsZero() && pod.DeletionTimestamp.IsZero() {
					terminationTarget := slices.IndexFunc(got.Status.Conditions, func(c corev1.PodCondition) bool {
						return c.Type == corev1.PodConditionType(ConditionTypeTerminationTarget)
					})
					if terminationTarget < 0 {
						t.Errorf("pod %s: deleted without a termination condition", pod.Name)
					} else if reason := got.Status.Conditions[terminationTarget].Reason; reason != ReasonTopologyDomainFailover {
						t.Errorf("pod %s: termination reason = %q, want %q", pod.Name, reason, ReasonTopologyDomainFailover)
					}
					gotStranded = append(gotStranded, pod.Name)
				}
			}
			if diff := cmp.Diff(tc.wantStranded, gotStranded, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("deleted members (-want +got):\n%s", diff)
			}
			var gotEvents []string
			for _, ev := range recorder.RecordedEvents {
				if ev.Reason == ReasonStrandedPodDeleted {
					gotEvents = append(gotEvents, ev.Key.Name)
				}
			}
			if diff := cmp.Diff(tc.wantStranded, gotEvents, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("StrandedPodDeleted events (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStrandedMemberCountsAsFailed(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlicesForPodGroups: true})
	ctx, _ := utiltesting.ContextWithLog(t)
	// A group of two: one member succeeded, the other was deleted by a domain
	// failover and also ended with exit code 0.
	succeeded := elasticGroupMember("grp-0").GroupTotalCount("2").UID("grp-0").KueueFinalizer().RoleHash(elasticTestRoleHash).
		NodeName("y1").StatusPhase(corev1.PodSucceeded).Obj()
	stranded := elasticGroupMember("grp-1").GroupTotalCount("2").UID("grp-1").KueueFinalizer().RoleHash(elasticTestRoleHash).
		NodeName("x1").StatusPhase(corev1.PodSucceeded).DeletionTimestamp(now).
		StatusConditions(corev1.PodCondition{Type: corev1.PodConditionType(ConditionTypeTerminationTarget), Status: corev1.ConditionTrue, Reason: ReasonTopologyDomainFailover}).Obj()
	group := elasticGroup(succeeded, stranded)

	reclaimable, err := group.ReclaimablePods(ctx, nil)
	if err != nil {
		t.Fatalf("ReclaimablePods() error = %v", err)
	}
	want := []kueue.ReclaimablePod{{Name: kueue.NewPodSetReference(elasticTestRoleHash), Count: 1}}
	if diff := cmp.Diff(want, reclaimable); diff != "" {
		t.Errorf("ReclaimablePods() (-want +got):\n%s", diff)
	}
	if _, _, finished := group.Finished(ctx); finished {
		t.Errorf("Finished() = true, want the group to wait for the stranded member's replacement")
	}
}

func TestConstructComposableWorkloadProbeTemplate(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
		features.ElasticJobsViaWorkloadSlices:                          true,
		features.ElasticJobsViaWorkloadSlicesForPodGroups:              true,
		features.ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp: true,
	})
	ctx, _ := utiltesting.ContextWithLog(t)
	// Every member has been released, so each carries the node selector the
	// ungaters injected.
	released := func(name, node string) *corev1.Pod {
		return elasticGroupMember(name).RoleHash("role-a").
			Annotation(kueueconstants.ElasticJobScaleUpStrategyAnnotationKey, kueueconstants.ElasticJobScaleUpStrategyPartial).
			NodeSelector("kubernetes.io/hostname", node).
			NodeSelector("example.com/pool", "gpu").
			StatusPhase(corev1.PodRunning).Obj()
	}
	group := elasticGroup(released("grp-0", "node-a"), released("grp-1", "node-b"), released("grp-2", "node-c"), released("grp-3", "node-d"))
	// The admitted slice keeps the template the group was created with.
	admitted := utiltestingapi.MakeWorkload(elasticTestGroup, elasticTestNamespace).
		Annotation(kueue.WorkloadSliceNameAnnotation, elasticTestGroup).
		PodSets(*utiltestingapi.MakePodSet("role-a", 4).
			SetMinimumCount(2).
			NodeSelector(map[string]string{"example.com/pool": "gpu"}).
			Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").
			PodSets(utiltestingapi.MakePodSetAssignment("role-a").Count(2).Obj()).
			Obj(), time.Now()).
		Obj()
	cli := utiltesting.NewClientBuilder().
		WithObjects(admitted).
		WithIndex(&kueue.Workload{}, indexer.WorkloadSliceNameKey, indexer.IndexWorkloadSliceName).
		Build()

	wl, err := group.ConstructComposableWorkload(ctx, cli, &utiltesting.EventRecorder{}, nil, nil)
	if err != nil {
		t.Fatalf("ConstructComposableWorkload() error = %v", err)
	}
	if want := elasticTestGroup + "-scale-up-probe-2"; wl.Name != want {
		t.Errorf("probe name = %q, want %q", wl.Name, want)
	}
	if len(wl.Spec.PodSets) != 1 {
		t.Fatalf("got %d PodSets, want 1", len(wl.Spec.PodSets))
	}
	if diff := cmp.Diff(map[string]string{"example.com/pool": "gpu"}, wl.Spec.PodSets[0].Template.Spec.NodeSelector); diff != "" {
		t.Errorf("probe template nodeSelector mismatch (-want,+got):\n%s", diff)
	}
	if got := ptr.Deref(wl.Spec.PodSets[0].MinCount, 0); got != 2 {
		t.Errorf("probe minCount = %d, want 2", got)
	}
}
