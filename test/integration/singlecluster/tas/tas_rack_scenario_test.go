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
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/tas"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/metrics"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/test/integration/framework"
	"sigs.k8s.io/kueue/test/util/behavioral"
	"sigs.k8s.io/kueue/test/util/behavioral/integration"
)

// rackSpec names a rack and how many identical nodes it holds.
type rackSpec struct {
	name  string
	nodes int
}

// rackScenario is a one-block topology of named racks on the envtest API server,
// with the real scheduler, TAS controllers and webhooks running against it. Every
// node has one CPU, so a pod requesting one CPU takes a whole node, and the
// ClusterQueue preempts lower-priority Workloads within the queue.
type rackScenario struct {
	ns           *corev1.Namespace
	nodes        []corev1.Node
	rackOfNode   map[string]string
	nodesOfRack  map[string][]string
	topology     *kueue.Topology
	tasFlavor    *kueue.ResourceFlavor
	clusterQueue *kueue.ClusterQueue
	localQueue   *kueue.LocalQueue
}

func newRackScenario(ns *corev1.Namespace, racks ...rackSpec) *rackScenario {
	ginkgo.GinkgoHelper()
	s := &rackScenario{
		ns:          ns,
		rackOfNode:  make(map[string]string),
		nodesOfRack: make(map[string][]string),
	}
	totalNodes := 0
	for _, rack := range racks {
		for i := range rack.nodes {
			name := fmt.Sprintf("%s-%02d", rack.name, i)
			s.nodes = append(s.nodes, *testingnode.MakeNode(name).
				Label("node-group", "tas").
				Label(utiltesting.DefaultBlockTopologyLevel, "b1").
				Label(utiltesting.DefaultRackTopologyLevel, rack.name).
				Label(corev1.LabelHostname, name).
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:  resource.MustParse("1"),
					corev1.ResourcePods: resource.MustParse("10"),
				}).
				Ready().
				Obj())
			s.rackOfNode[name] = rack.name
			s.nodesOfRack[rack.name] = append(s.nodesOfRack[rack.name], name)
		}
		totalNodes += rack.nodes
	}
	integration.CreateNodesWithStatus(ctx, k8sClient, s.nodes)

	s.topology = utiltestingapi.MakeDefaultThreeLevelTopology("default")
	behavioral.MustCreate(ctx, k8sClient, s.topology)

	s.tasFlavor = utiltestingapi.MakeResourceFlavor("tas-flavor").
		NodeLabel("node-group", "tas").
		TopologyName(s.topology.Name).Obj()
	behavioral.MustCreate(ctx, k8sClient, s.tasFlavor)

	// Quota covers every node, so placement alone decides.
	s.clusterQueue = utiltestingapi.MakeClusterQueue("cluster-queue").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas(s.tasFlavor.Name).Resource(corev1.ResourceCPU, strconv.Itoa(totalNodes)).Obj()).
		Preemption(kueue.ClusterQueuePreemption{WithinClusterQueue: kueue.PreemptionPolicyLowerPriority}).
		Obj()
	behavioral.MustCreate(ctx, k8sClient, s.clusterQueue)
	behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, s.clusterQueue)

	s.localQueue = utiltestingapi.MakeLocalQueue("local-queue", ns.Name).ClusterQueue(s.clusterQueue.Name).Obj()
	behavioral.MustCreate(ctx, k8sClient, s.localQueue)
	return s
}

func (s *rackScenario) teardown() {
	ginkgo.GinkgoHelper()
	gomega.Expect(behavioral.DeleteWorkloadsInNamespace(ctx, k8sClient, s.ns)).Should(gomega.Succeed())
	gomega.Expect(behavioral.DeleteObject(ctx, k8sClient, s.localQueue)).Should(gomega.Succeed())
	behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, s.clusterQueue, true)
	behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, s.tasFlavor, true)
	behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, s.topology, true)
	for i := range s.nodes {
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, &s.nodes[i], true)
	}
	metrics.ClearClusterQueueMetrics(kueue.ClusterQueueReference(s.clusterQueue.Name))
}

// occupant is a Workload of one-CPU pods pinned to one rack through a node selector.
func (s *rackScenario) occupant(name, rack string, pods int, priority int32) *kueue.Workload {
	return utiltestingapi.MakeWorkload(name, s.ns.Name).
		Queue(kueue.LocalQueueName(s.localQueue.Name)).
		Priority(priority).
		PodSets(*utiltestingapi.MakePodSet("main", pods).
			RequiredTopologyRequest(utiltesting.DefaultRackTopologyLevel).
			NodeSelector(map[string]string{utiltesting.DefaultRackTopologyLevel: rack}).
			Request(corev1.ResourceCPU, "1").Obj()).
		Obj()
}

// elasticJob is an elastic Workload opted into rack failover, with `roles`
// rack-bound PodSets named r0, r1, ... of `pods` one-CPU pods each, each
// admissible down to `minPods`.
func (s *rackScenario) elasticJob(name string, roles, pods int, minPods int32, priority int32) *kueue.Workload {
	podSets := make([]kueue.PodSet, 0, roles)
	for i := range roles {
		podSets = append(podSets, *utiltestingapi.MakePodSet(kueue.PodSetReference(fmt.Sprintf("r%d", i)), pods).
			RequiredTopologyRequest(utiltesting.DefaultRackTopologyLevel).
			SetMinimumCount(minPods).
			Request(corev1.ResourceCPU, "1").Obj())
	}
	return utiltestingapi.MakeWorkload(name, s.ns.Name).
		Queue(kueue.LocalQueueName(s.localQueue.Name)).
		Priority(priority).
		Annotation(constants.ElasticJobAnnotation, "true").
		Annotation(constants.ElasticJobDomainFailoverAnnotationKey, constants.ElasticJobDomainFailoverAnnotationValue).
		PodSets(podSets...).
		Obj()
}

// admit creates the Workload and waits for its admission.
func (s *rackScenario) admit(wl *kueue.Workload) *kueue.Workload {
	ginkgo.GinkgoHelper()
	behavioral.MustCreate(ctx, k8sClient, wl)
	behavioral.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, wl)
	return wl
}

// rackOfDomain resolves the rack of an assigned domain, whether the assignment
// carries the rack level or only hostnames.
func (s *rackScenario) rackOfDomain(levels, values []string) (string, bool) {
	if i := slices.Index(levels, utiltesting.DefaultRackTopologyLevel); i >= 0 && i < len(values) {
		return values[i], true
	}
	if i := slices.Index(levels, corev1.LabelHostname); i >= 0 && i < len(values) {
		rack, ok := s.rackOfNode[values[i]]
		return rack, ok
	}
	return "", false
}

// roleRacks re-reads the Workload and returns, per admitted PodSet, how many of
// its pods sit in each rack.
func (s *rackScenario) roleRacks(g gomega.Gomega, wl *kueue.Workload) map[kueue.PodSetReference]map[string]int32 {
	g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), wl)).To(gomega.Succeed())
	g.Expect(wl.Status.Admission).NotTo(gomega.BeNil(), "workload %s has no admission", wl.Name)
	result := make(map[kueue.PodSetReference]map[string]int32, len(wl.Status.Admission.PodSetAssignments))
	for _, psa := range wl.Status.Admission.PodSetAssignments {
		g.Expect(psa.TopologyAssignment).NotTo(gomega.BeNil(), "PodSet %s has no topology assignment", psa.Name)
		assignment := utiltas.InternalFrom(psa.TopologyAssignment)
		racks := make(map[string]int32)
		for _, domain := range assignment.Domains {
			rack, ok := s.rackOfDomain(assignment.Levels, domain.Values)
			g.Expect(ok).To(gomega.BeTrue(), "domain %v of PodSet %s is in no known rack", domain.Values, psa.Name)
			racks[rack] += domain.Count
		}
		result[psa.Name] = racks
	}
	return result
}

// expectRoleInRackNow asserts that the PodSet's whole admitted count sits in the rack.
func (s *rackScenario) expectRoleInRackNow(g gomega.Gomega, wl *kueue.Workload, role kueue.PodSetReference, rack string) {
	racks := s.roleRacks(g, wl)[role]
	psa := podSetAssignmentByName(wl, role)
	g.Expect(psa).NotTo(gomega.BeNil(), "no pod set assignment for PodSet %s", role)
	g.Expect(racks).To(gomega.Equal(map[string]int32{rack: ptr.Deref(psa.Count, 0)}), "PodSet %s is not entirely in rack %s", role, rack)
}

// expectRoleInRack waits until the PodSet's whole admitted count sits in the rack.
func (s *rackScenario) expectRoleInRack(wl *kueue.Workload, role kueue.PodSetReference, rack string, timeout time.Duration) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		s.expectRoleInRackNow(g, wl, role, rack)
	}, timeout, behavioral.Interval).Should(gomega.Succeed())
}

// expectRoleStaysInRack asserts that the PodSet keeps its whole admitted count in
// the rack for long enough to cover a second-pass retry.
func (s *rackScenario) expectRoleStaysInRack(wl *kueue.Workload, role kueue.PodSetReference, rack string) {
	ginkgo.GinkgoHelper()
	gomega.Consistently(func(g gomega.Gomega) {
		s.expectRoleInRackNow(g, wl, role, rack)
	}, behavioral.LongConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
}

// expectRolesInRacks waits until every PodSet sits entirely in one rack and the
// PodSets together occupy exactly the given racks, and returns the rack of each.
func (s *rackScenario) expectRolesInRacks(wl *kueue.Workload, racks ...string) map[kueue.PodSetReference]string {
	ginkgo.GinkgoHelper()
	result := make(map[kueue.PodSetReference]string)
	gomega.Eventually(func(g gomega.Gomega) {
		clear(result)
		for role, spread := range s.roleRacks(g, wl) {
			g.Expect(spread).To(gomega.HaveLen(1), "PodSet %s spans racks %v", role, spread)
			for rack := range spread {
				result[role] = rack
			}
		}
		occupied := sets.New[string]()
		for _, rack := range result {
			occupied.Insert(rack)
		}
		g.Expect(occupied).To(gomega.Equal(sets.New(racks...)))
	}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	return result
}

// roleInRack returns the PodSet that expectRolesInRacks placed in the rack.
func roleInRack(rackOfRole map[kueue.PodSetReference]string, rack string) kueue.PodSetReference {
	ginkgo.GinkgoHelper()
	for role, r := range rackOfRole {
		if r == rack {
			return role
		}
	}
	ginkgo.Fail(fmt.Sprintf("no PodSet is in rack %s: %v", rack, rackOfRole))
	return ""
}

// roleCount returns the admitted count of the PodSet.
func roleCount(wl *kueue.Workload, role kueue.PodSetReference) int32 {
	ginkgo.GinkgoHelper()
	psa := podSetAssignmentByName(wl, role)
	gomega.Expect(psa).NotTo(gomega.BeNil(), "no pod set assignment for PodSet %s", role)
	return ptr.Deref(psa.Count, 0)
}

// failNode marks the node NotReady since before the node failure delay, so the
// TAS node controller treats it as failed on its next reconcile.
func (s *rackScenario) failNode(name string) {
	ginkgo.GinkgoHelper()
	node := &corev1.Node{}
	gomega.Expect(k8sClient.Get(ctx, apitypes.NamespacedName{Name: name}, node)).Should(gomega.Succeed())
	behavioral.SetNodeCondition(ctx, k8sClient, node, &corev1.NodeCondition{
		Type:               corev1.NodeReady,
		Status:             corev1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-tas.NodeFailureDelay)),
	})
}

// expectUnhealthyNodes waits until the admitted Workload records exactly these
// unhealthy nodes, in this order.
func (s *rackScenario) expectUnhealthyNodes(wl *kueue.Workload, names ...string) {
	ginkgo.GinkgoHelper()
	expected := make([]kueue.UnhealthyNode, 0, len(names))
	for _, name := range names {
		expected = append(expected, kueue.UnhealthyNode{Name: name})
	}
	gomega.Eventually(func(g gomega.Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), wl)).To(gomega.Succeed())
		g.Expect(workload.IsAdmitted(wl)).To(gomega.BeTrue())
		g.Expect(wl.Status.UnhealthyNodes).To(gomega.BeComparableTo(expected, cmpopts.EquateEmpty()))
	}, behavioral.LongTimeout, behavioral.Interval).Should(gomega.Succeed())
}

// expectPreempted waits until each Workload is evicted by preemption. The wait
// is long because a second pass retries with a backoff of up to 30 seconds.
func (s *rackScenario) expectPreempted(wls ...*kueue.Workload) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		for _, wl := range wls {
			updated := &kueue.Workload{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), updated)).To(gomega.Succeed())
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, kueue.WorkloadEvicted)
			g.Expect(cond).NotTo(gomega.BeNil(), "workload %s is not evicted", wl.Name)
			g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue), "workload %s is not evicted", wl.Name)
			g.Expect(cond.Reason).To(gomega.Equal(kueue.WorkloadPreempted), "workload %s was evicted for another reason", wl.Name)
		}
	}, behavioral.LongTimeout, behavioral.Interval).Should(gomega.Succeed())
}

// remove deletes the Workloads the way their jobs' removal would, clearing the
// finalizer Kueue holds while quota is reserved, and waits until they are gone.
func (s *rackScenario) remove(wls ...*kueue.Workload) {
	ginkgo.GinkgoHelper()
	for _, wl := range wls {
		gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, wl))).To(gomega.Succeed())
	}
	gomega.Eventually(func(g gomega.Gomega) {
		for _, wl := range wls {
			updated := &kueue.Workload{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), updated)
			if apierrors.IsNotFound(err) {
				continue
			}
			g.Expect(err).NotTo(gomega.HaveOccurred())
			if controllerutil.RemoveFinalizer(updated, kueue.ResourceInUseFinalizerName) {
				g.Expect(client.IgnoreNotFound(k8sClient.Update(ctx, updated))).To(gomega.Succeed())
			}
			g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), updated))).To(gomega.BeTrue(), "workload %s still exists", wl.Name)
		}
	}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
}

// eventCount sums how often the Workload received an event with the reason,
// counting an aggregated series by its observed count.
func (s *rackScenario) eventCount(wl *kueue.Workload, reason string) (int, error) {
	events, err := behavioral.EventsForObject(ctx, k8sClient, client.ObjectKeyFromObject(wl))
	if err != nil {
		return 0, err
	}
	total := 0
	for _, event := range events {
		if event.Reason != reason {
			continue
		}
		if event.Series != nil {
			total += int(event.Series.Count)
		} else {
			total++
		}
	}
	return total, nil
}

// eventCountNow is eventCount for use between steps, failing the spec on a list error.
func (s *rackScenario) eventCountNow(wl *kueue.Workload, reason string) int {
	ginkgo.GinkgoHelper()
	count, err := s.eventCount(wl, reason)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return count
}

// expectEventSince waits until the Workload has received the event more often than before.
func (s *rackScenario) expectEventSince(wl *kueue.Workload, reason string, before int) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		count, err := s.eventCount(wl, reason)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(count).To(gomega.BeNumerically(">", before), "no new %s event for workload %s", reason, wl.Name)
	}, behavioral.LongTimeout, behavioral.Interval).Should(gomega.Succeed())
}

// expectFailoverEvent waits for the event reporting that the PodSet moved from
// one rack to another, placed with `count` of its `admitted` pods, after its
// rack could run only `healthy` of the `minimum` pods it needs.
func (s *rackScenario) expectFailoverEvent(role kueue.PodSetReference, from, to string, count, admitted, healthy, minimum int32) {
	ginkgo.GinkgoHelper()
	placed := ""
	if count < admitted {
		placed = fmt.Sprintf(" at %d of its %d pods", count, admitted)
	}
	fromDomain, toDomain := "b1,"+from, "b1,"+to
	behavioral.ExpectEventAppeared(ctx, k8sClient, eventsv1.Event{
		Reason: "TopologyDomainFailover",
		Type:   corev1.EventTypeNormal,
		Note: fmt.Sprintf("PodSet %s moved from %s %q to %q%s: %q can run %d of the %d pods the PodSet needs at minimum",
			role, utiltesting.DefaultRackTopologyLevel, fromDomain, toDomain, placed, fromDomain, healthy, minimum),
	})
}

var _ = ginkgo.Describe("Topology Aware Scheduling rack scenarios", ginkgo.Ordered, func() {
	var (
		ns *corev1.Namespace
		s  *rackScenario
	)

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerSetup())
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.ElasticJobsViaWorkloadSlicesDomainFailover, true)
		// Lets an elastic Workload carry minCount on several PodSets.
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp, true)
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.TASReplaceMultipleFailedNodes, true)
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.TASFailedNodeReplacementFailFast, false)
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "tas-racks-")
		s = nil
	})

	ginkgo.AfterEach(func() {
		if s != nil {
			s.teardown()
		}
		gomega.Expect(forceDeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
	})

	// A two-role job of 18 nodes per role, admissible down to 16, lands with one role
	// per rack. A rack short of 18 nodes takes its role at the largest count that
	// fits, down to the minimum.
	ginkgo.DescribeTable("admits a two-role job of 18 nodes per role, admissible down to 16, into two racks",
		func(nodesInA, nodesInB int, wantInA, wantInB int32) {
			s = newRackScenario(ns, rackSpec{"a", nodesInA}, rackSpec{"b", nodesInB})
			job := s.admit(s.elasticJob("job", 2, 18, 16, 10))
			rackOfRole := s.expectRolesInRacks(job, "a", "b")
			gomega.Expect(roleCount(job, roleInRack(rackOfRole, "a"))).To(gomega.Equal(wantInA), "count of the role in rack a")
			gomega.Expect(roleCount(job, roleInRack(rackOfRole, "b"))).To(gomega.Equal(wantInB), "count of the role in rack b")
		},
		ginkgo.Entry("two racks of 18 take both roles in full", 18, 18, int32(18), int32(18)),
		ginkgo.Entry("a rack of 16 takes its role trimmed to the minimum of 16", 18, 16, int32(18), int32(16)),
		ginkgo.Entry("a rack of 17 takes its role at 17, the largest count that fits", 18, 17, int32(18), int32(17)),
		ginkgo.Entry("two racks of 16 take both roles trimmed to the minimum of 16", 16, 16, int32(16), int32(16)),
	)

	// The quota covers the whole request, so only the rack minimum keeps the job pending.
	ginkgo.It("keeps a two-role job pending while only one rack can reach the minimum of 16", func() {
		s = newRackScenario(ns, rackSpec{"a", 18}, rackSpec{"b", 15}, rackSpec{"c", 3})
		job := s.elasticJob("job", 2, 18, 16, 10)
		behavioral.MustCreate(ctx, k8sClient, job)
		behavioral.ExpectWorkloadsToBePending(ctx, k8sClient, job)
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)).To(gomega.Succeed())
			g.Expect(workload.HasQuotaReservation(job)).To(gomega.BeFalse())
			cond := apimeta.FindStatusCondition(job.Status.Conditions, kueue.WorkloadQuotaReserved)
			g.Expect(cond).NotTo(gomega.BeNil())
			g.Expect(cond.Reason).To(gomega.Equal(kueue.WorkloadQuotaReservedReasonTopologyPlacementFailed), cond.Message)
		}, behavioral.LongConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
	})

	const lowPriority, jobPriority, highPriority = 1, 10, 20

	// loseRackB admits the given number of single-node occupants at the given
	// priority into rack c, then a two-role job of 18 nodes per role, admissible
	// down to 16, at priority 10 into racks a and b, and fails three rack-b nodes
	// one at a time. The first two failures keep the role at or above its minimum,
	// so it stays; the third drops it to 15, below the minimum. It returns the job,
	// the occupants and the PodSets in racks b and a.
	loseRackB := func(rackC rackSpec, occupantCount int, occupantPriority int32) (job *kueue.Workload, occupants []*kueue.Workload, moving, staying kueue.PodSetReference) {
		ginkgo.GinkgoHelper()
		s = newRackScenario(ns, rackSpec{"a", 18}, rackSpec{"b", 18}, rackC)

		if occupantCount > 0 {
			ginkgo.By(fmt.Sprintf("admitting %d single-node occupants at priority %d into rack c", occupantCount, occupantPriority), func() {
				for i := range occupantCount {
					occupant := s.admit(s.occupant(fmt.Sprintf("occupant-%d", i), "c", 1, occupantPriority))
					s.expectRoleInRack(occupant, "main", "c", behavioral.Timeout)
					occupants = append(occupants, occupant)
				}
			})
		}

		var rackOfRole map[kueue.PodSetReference]string
		ginkgo.By("admitting a two-role job of 18 nodes per role with a minimum of 16 into racks a and b", func() {
			job = s.admit(s.elasticJob("job", 2, 18, 16, jobPriority))
			rackOfRole = s.expectRolesInRacks(job, "a", "b")
		})
		moving = roleInRack(rackOfRole, "b")
		staying = roleInRack(rackOfRole, "a")
		failed := s.nodesOfRack["b"][:3]

		ginkgo.By("failing two rack-b nodes one at a time, which keeps the role at or above its minimum of 16", func() {
			for i := range 2 {
				before := s.eventCountNow(job, "SecondPassFailed")
				s.failNode(failed[i])
				s.expectUnhealthyNodes(job, failed[:i+1]...)
				s.expectEventSince(job, "SecondPassFailed", before)
				s.expectRoleStaysInRack(job, moving, "b")
			}
		})

		ginkgo.By("failing a third rack-b node, which drops the role to 15", func() {
			s.failNode(failed[2])
			s.expectUnhealthyNodes(job, failed...)
		})
		return job, occupants, moving, staying
	}

	ginkgo.It("moves a role to the rack of preempted lower-priority occupants once three of its nodes fail", framework.SlowSpec, func() {
		job, occupants, moving, staying := loseRackB(rackSpec{"c", 18}, 2, lowPriority)

		ginkgo.By("preempting the occupants of rack c, which fits the whole role once they are gone", func() {
			s.expectPreempted(occupants...)
		})

		ginkgo.By("removing the preempted occupants, as the deletion of their jobs would", func() {
			s.remove(occupants...)
		})

		ginkgo.By("moving the role to rack c at its admitted count of 18 and leaving the other role in rack a", func() {
			s.expectRoleInRack(job, moving, "c", behavioral.LongTimeout)
			gomega.Expect(roleCount(job, moving)).To(gomega.Equal(int32(18)))
			s.expectRoleInRack(job, staying, "a", behavioral.Timeout)
			s.expectUnhealthyNodes(job)
			s.expectFailoverEvent(moving, "b", "c", 18, 18, 15, 16)
		})
	})

	// A spare rack smaller than the role takes it at the most pods it fits, down to
	// the role's minimum.
	ginkgo.DescribeTable("moves a role to a smaller spare rack at the most pods it fits once three of its nodes fail", framework.SlowSpec,
		func(nodesInC int) {
			job, _, moving, staying := loseRackB(rackSpec{"c", nodesInC}, 0, 0)

			ginkgo.By(fmt.Sprintf("moving the role to rack c at %d of its 18 pods and leaving the other role in rack a", nodesInC), func() {
				s.expectRoleInRack(job, moving, "c", behavioral.LongTimeout)
				gomega.Expect(roleCount(job, moving)).To(gomega.Equal(int32(nodesInC)))
				s.expectRoleInRack(job, staying, "a", behavioral.Timeout)
				gomega.Expect(roleCount(job, staying)).To(gomega.Equal(int32(18)))
				s.expectUnhealthyNodes(job)
				s.expectFailoverEvent(moving, "b", "c", int32(nodesInC), 18, 15, 16)
			})
		},
		ginkgo.Entry("a rack of 16 takes the role at its minimum of 16", 16),
		ginkgo.Entry("a rack of 17 takes the role at 17", 17),
	)

	ginkgo.It("moves a role at fewer pods rather than preempting occupants that outrank it", framework.SlowSpec, func() {
		job, occupants, moving, staying := loseRackB(rackSpec{"c", 18}, 2, highPriority)

		ginkgo.By("moving the role to the 16 free nodes of rack c and leaving the other role in rack a", func() {
			s.expectRoleInRack(job, moving, "c", behavioral.LongTimeout)
			gomega.Expect(roleCount(job, moving)).To(gomega.Equal(int32(16)))
			s.expectRoleInRack(job, staying, "a", behavioral.Timeout)
			s.expectUnhealthyNodes(job)
			s.expectFailoverEvent(moving, "b", "c", 16, 18, 15, 16)
		})

		ginkgo.By("leaving the higher-priority occupants admitted", func() {
			gomega.Consistently(func(g gomega.Gomega) {
				for _, occupant := range occupants {
					g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(occupant), occupant)).To(gomega.Succeed())
					g.Expect(workload.IsAdmitted(occupant)).To(gomega.BeTrue(), "occupant %s is no longer admitted", occupant.Name)
					g.Expect(apimeta.IsStatusConditionTrue(occupant.Status.Conditions, kueue.WorkloadEvicted)).To(gomega.BeFalse(), "occupant %s was evicted", occupant.Name)
				}
			}, behavioral.ConsistentDuration, behavioral.Interval).Should(gomega.Succeed())
		})
	})

	// The second pass retries on a backoff timer instead of standing at the head of
	// its ClusterQueue, so occupants requeued right after their eviction are admitted
	// into rack c again before the role moves, and the role preempts them again.
	// Observed 2026-10-28: both occupants were re-admitted 10 ms after their
	// reservations were released, while the role's retry was 8 s away.
	ginkgo.PIt("moves a role before re-admitting the lower-priority occupants it preempted", framework.SlowSpec, func() {
		job, occupants, moving, staying := loseRackB(rackSpec{"c", 18}, 2, lowPriority)

		ginkgo.By("preempting the occupants of rack c, which fits the whole role once they are gone", func() {
			s.expectPreempted(occupants...)
		})

		ginkgo.By("releasing the occupants' reservations, as their job controllers would, which requeues them", func() {
			behavioral.FinishEvictionForWorkloads(ctx, k8sClient, occupants...)
		})

		ginkgo.By("moving the role to rack c before the occupants are admitted again", func() {
			s.expectRoleInRack(job, moving, "c", behavioral.LongTimeout)
			s.expectRoleInRack(job, staying, "a", behavioral.Timeout)
			for _, occupant := range occupants {
				gomega.Expect(s.eventCountNow(occupant, "Admitted")).To(gomega.Equal(1), "occupant %s was admitted again before the role moved", occupant.Name)
			}
		})
	})
})
