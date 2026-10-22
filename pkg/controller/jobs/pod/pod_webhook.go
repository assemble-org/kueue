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
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	"sigs.k8s.io/kueue/pkg/constants"
	ctrlconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	metaPath                       = field.NewPath("metadata")
	labelsPath                     = metaPath.Child("labels")
	annotationsPath                = metaPath.Child("annotations")
	managedLabelPath               = labelsPath.Key(constants.ManagedByKueueLabelKey)
	groupNameLabelPath             = labelsPath.Key(podconstants.GroupNameLabel)
	groupNameAnnotationPath        = annotationsPath.Key(podconstants.GroupNameAnnotation)
	groupTotalCountAnnotationPath  = annotationsPath.Key(podconstants.GroupTotalCountAnnotation)
	retriableInGroupAnnotationPath = annotationsPath.Key(podconstants.RetriableInGroupAnnotationKey)
)

type PodWebhook struct {
	integrationManager           *jobframework.IntegrationManager
	client                       client.Client
	queues                       *qcache.Manager
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	namespaceSelector            *metav1.LabelSelector
	podSelector                  *metav1.LabelSelector
	maxTimeoutOnWorkload         *metav1.Duration
}

// SetupWebhook configures the webhook for pods.
func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &PodWebhook{
		integrationManager:           options.IntegrationManager,
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		maxTimeoutOnWorkload:         options.MaxTimeoutOnWorkload,
	}
	obj := &corev1.Pod{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithDefaulter(wh).
		WithValidator(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(FromObject(obj).GVK(), options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

var _ admission.Defaulter[*corev1.Pod] = &PodWebhook{}

// addRoleHash calculates the role hash and adds it to the pod's annotations
func (p *Pod) addRoleHash() error {
	if p.pod.Annotations == nil {
		p.pod.Annotations = make(map[string]string)
	}

	hash, err := getRoleHash(p.pod)
	if err != nil {
		return err
	}

	p.pod.Annotations[podconstants.RoleHashAnnotation] = hash
	return nil
}

func (w *PodWebhook) Default(ctx context.Context, obj *corev1.Pod) error {
	pod := FromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("pod-webhook")
	log.V(5).Info("Applying defaults")

	_, suspendByParent := pod.pod.GetAnnotations()[podconstants.SuspendedByParentAnnotation]

	suspend := suspendByParent
	if !suspend {
		// Namespace filtering
		ns := corev1.Namespace{}
		err := w.client.Get(ctx, client.ObjectKey{Name: pod.pod.GetNamespace()}, &ns)
		if err != nil {
			return fmt.Errorf("failed to get namespace: %w", err)
		}
		if w.managedJobsNamespaceSelector != nil && !w.managedJobsNamespaceSelector.Matches(labels.Set(ns.GetLabels())) {
			return nil
		}

		// Backwards compatibility: podOptions.podSelector
		if w.podSelector != nil {
			podSelector, err := metav1.LabelSelectorAsSelector(w.podSelector)
			if err != nil {
				return fmt.Errorf("failed to parse pod selector: %w", err)
			}
			if !podSelector.Matches(labels.Set(pod.pod.GetLabels())) {
				return nil
			}
		}

		// Backwards compatibility: podOptions.namespaceSelector
		if w.namespaceSelector != nil {
			nsSelector, err := metav1.LabelSelectorAsSelector(w.namespaceSelector)
			if err != nil {
				return fmt.Errorf("failed to parse namespace selector: %w", err)
			}
			if !nsSelector.Matches(labels.Set(ns.GetLabels())) {
				return nil
			}
		}

		// Do not suspend a Pod whose owner is already managed by Kueue
		ancestorJob, err := w.integrationManager.FindAncestorJobManagedByKueue(ctx, w.client, pod.Object(), w.manageJobsWithoutQueueName)
		if err != nil || ancestorJob != nil {
			return err
		}

		// Local queue defaulting
		if jobframework.QueueNameForObject(pod.Object()) == "" &&
			w.queues.DefaultLocalQueueExist(pod.pod.GetNamespace()) {
			if pod.pod.Labels == nil {
				pod.pod.Labels = make(map[string]string)
			}
			pod.pod.Labels[ctrlconstants.QueueLabel] = string(ctrlconstants.DefaultLocalQueueName)
		}

		// The namespace selector was already enforced above, so it is not re-checked here.
		if err := w.integrationManager.ApplyDefaultWorkloadPriorityClass(ctx, w.client, pod.Object(), nil); err != nil {
			return err
		}

		suspend = jobframework.QueueNameForObject(pod.Object()) != "" || w.manageJobsWithoutQueueName
		if suspend {
			if pod.pod.Labels == nil {
				pod.pod.Labels = make(map[string]string)
			}
			pod.pod.Labels[constants.ManagedByKueueLabelKey] = constants.ManagedByKueueLabelValue
		}
	}

	if suspend {
		if !suspendByParent {
			controllerutil.AddFinalizer(pod.Object(), podconstants.PodFinalizer)
		}

		gate(&pod.pod)

		if features.Enabled(features.TopologyAwareScheduling) {
			if val, ok := pod.pod.Annotations[kueue.PodGroupPodIndexLabelAnnotation]; ok {
				if pod.pod.Labels == nil {
					pod.pod.Labels = make(map[string]string, 1)
				}
				pod.pod.Labels[kueue.PodGroupPodIndexLabel] = pod.pod.Labels[val]
			}
			utilpod.Gate(&pod.pod, kueue.TopologySchedulingGate)
		}
		if err := pod.addRoleHash(); err != nil {
			return err
		}
		if pod.isElasticGroupMember() {
			pod.prepareElasticGroupMember()
		}
		// copy back changes to the object
		pod.pod.DeepCopyInto(obj)
	}

	return nil
}

// +kubebuilder:webhook:path=/validate--v1-pod,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create;update,versions=v1,name=vpod.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*corev1.Pod] = &PodWebhook{}

func (w *PodWebhook) ValidateCreate(ctx context.Context, obj *corev1.Pod) (admission.Warnings, error) {
	var warnings admission.Warnings

	pod := FromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("pod-webhook")
	log.V(5).Info("Validating create")

	allErrs := jobframework.ValidateJobOnCreate(pod, w.maxTimeoutOnWorkload)
	allErrs = append(allErrs, validateCommon(pod)...)

	if warn := warningForPodManagedLabel(w.integrationManager, pod); warn != "" {
		warnings = append(warnings, warn)
	}

	return warnings, allErrs.ToAggregate()
}

func (w *PodWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj *corev1.Pod) (admission.Warnings, error) {
	var warnings admission.Warnings

	oldPod := FromObject(oldObj)
	newPod := FromObject(newObj)
	log := ctrl.LoggerFrom(ctx).WithName("pod-webhook")
	log.V(5).Info("Validating update")

	allErrs := jobframework.ValidateJobOnUpdate(oldPod, newPod, w.queues.DefaultLocalQueueExist, w.maxTimeoutOnWorkload)
	allErrs = append(allErrs, validateCommon(newPod)...)
	allErrs = append(allErrs, validateUpdateForRetriableInGroupAnnotation(oldPod, newPod)...)

	if oldGroupName := utilpod.GetPodGroupName(&oldPod.pod); oldGroupName != "" {
		newGroupName := utilpod.GetPodGroupName(&newPod.pod)
		allErrs = append(allErrs, validation.ValidateImmutableField(newGroupName, oldGroupName, getGroupNamePath(&newPod.pod))...)
	}
	allErrs = append(allErrs, validateUpdateForElasticGroupAnnotations(oldPod, newPod)...)

	if _, suspendByParent := newPod.pod.Annotations[podconstants.SuspendedByParentAnnotation]; !suspendByParent {
		if warn := warningForPodManagedLabel(w.integrationManager, newPod); warn != "" {
			warnings = append(warnings, warn)
		}
	}

	return warnings, allErrs.ToAggregate()
}

func (w *PodWebhook) ValidateDelete(context.Context, *corev1.Pod) (admission.Warnings, error) {
	return nil, nil
}

func validateCommon(pod *Pod) field.ErrorList {
	allErrs := validateManagedLabel(pod)
	allErrs = append(allErrs, validatePodGroupMetadata(pod)...)
	allErrs = append(allErrs, validateTopologyRequest(pod)...)
	allErrs = append(allErrs, validatePrebuiltWorkloadName(pod)...)
	return allErrs
}

func validateManagedLabel(pod *Pod) field.ErrorList {
	var allErrs field.ErrorList

	if managedLabel, ok := pod.pod.GetLabels()[constants.ManagedByKueueLabelKey]; ok && managedLabel != constants.ManagedByKueueLabelValue {
		return append(allErrs, field.Forbidden(managedLabelPath, fmt.Sprintf("managed label value can only be '%s'", constants.ManagedByKueueLabelValue)))
	}

	return allErrs
}

// warningForPodManagedLabel returns a warning message if the pod has a managed label, and it's parent is managed by kueue
func warningForPodManagedLabel(integrationManager *jobframework.IntegrationManager, p *Pod) string {
	managedLabel := p.pod.GetLabels()[constants.ManagedByKueueLabelKey]
	if managedLabel == constants.ManagedByKueueLabelValue && integrationManager.IsOwnerManagedByKueueForObject(p.Object()) {
		return fmt.Sprintf("pod owner is managed by kueue, label '%s=%s' might lead to unexpected behaviour",
			constants.ManagedByKueueLabelKey, constants.ManagedByKueueLabelValue)
	}

	return ""
}

func validatePodGroupMetadata(p *Pod) field.ErrorList {
	var allErrs field.ErrorList

	gtc, gtcExists := p.pod.GetAnnotations()[podconstants.GroupTotalCountAnnotation]

	errorDetail := fmt.Sprintf("both the '%s' annotation and the '%s' label should be set", podconstants.GroupTotalCountAnnotation, podconstants.GroupNameLabel)
	groupNameAnnotation := p.pod.Annotations[podconstants.GroupNameAnnotation]
	if features.Enabled(features.WorkloadIdentifierAnnotations) && groupNameAnnotation != "" {
		errorDetail = fmt.Sprintf("both the '%s' annotation and the '%s' annotation should be set", podconstants.GroupTotalCountAnnotation, podconstants.GroupNameAnnotation)
	}

	if groupName := utilpod.GetPodGroupName(&p.pod); groupName == "" {
		if gtcExists {
			return append(allErrs, field.Required(getGroupNamePath(&p.pod), errorDetail))
		}
	} else {
		allErrs = append(allErrs, validatePodGroupName(p.Object())...)

		if !gtcExists {
			return append(allErrs, field.Required(groupTotalCountAnnotationPath, errorDetail))
		}
	}

	if _, err := p.groupTotalCount(); gtcExists && err != nil {
		return append(allErrs, field.Invalid(groupTotalCountAnnotationPath, gtc, err.Error()))
	}

	return append(allErrs, validateElasticGroupMetadata(p)...)
}

var (
	groupMinCountAnnotationPath  = annotationsPath.Key(podconstants.GroupMinCountAnnotation)
	groupRoleCountAnnotationPath = annotationsPath.Key(podconstants.GroupRoleCountAnnotation)
	elasticJobAnnotationPath     = annotationsPath.Key(workloadslicing.EnabledAnnotationKey)
)

// validateElasticGroupMetadata checks the inputs a sliced pod group is built from:
// the elastic-job annotation needs a group, and the group minimum needs the
// elastic-job annotation and must stay within the group total count.
func validateElasticGroupMetadata(p *Pod) field.ErrorList {
	if !features.Enabled(features.ElasticJobsViaWorkloadSlicesForPodGroups) {
		return nil
	}
	var allErrs field.ErrorList
	elastic := workloadslicing.Enabled(&p.pod)
	if elastic && utilpod.GetPodGroupName(&p.pod) == "" {
		allErrs = append(allErrs, field.Forbidden(elasticJobAnnotationPath, "elastic job requires a pod group"))
	}
	requiresElastic := fmt.Sprintf("requires the %q annotation set to %q", workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
	totalCount, totalErr := p.groupTotalCount()
	// The minimum floors the PodSet its role becomes: the whole group for one
	// role, the role count when the group has several roles.
	minBound, minBoundName := totalCount, "group total count"
	if roleCount, found := p.pod.GetAnnotations()[podconstants.GroupRoleCountAnnotation]; found {
		switch parsed, err := strconv.Atoi(roleCount); {
		case !elastic:
			allErrs = append(allErrs, field.Forbidden(groupRoleCountAnnotationPath, requiresElastic))
		case err != nil || parsed < 1:
			allErrs = append(allErrs, field.Invalid(groupRoleCountAnnotationPath, roleCount, "role count should be an integer greater than zero"))
		case totalErr == nil && parsed > totalCount:
			allErrs = append(allErrs, field.Invalid(groupRoleCountAnnotationPath, roleCount,
				fmt.Sprintf("role count should not exceed the group total count %d", totalCount)))
		default:
			minBound, minBoundName = parsed, "role count"
		}
	}
	minCount, found := p.pod.GetAnnotations()[podconstants.GroupMinCountAnnotation]
	if !found {
		return allErrs
	}
	if !elastic {
		return append(allErrs, field.Forbidden(groupMinCountAnnotationPath, requiresElastic))
	}
	parsed, err := strconv.Atoi(minCount)
	if err != nil || parsed < 1 {
		return append(allErrs, field.Invalid(groupMinCountAnnotationPath, minCount, "group min count should be an integer greater than zero"))
	}
	if totalErr == nil && parsed > minBound {
		allErrs = append(allErrs, field.Invalid(groupMinCountAnnotationPath, minCount,
			fmt.Sprintf("group min count should not exceed the %s %d", minBoundName, minBound)))
	}
	return allErrs
}

// validateUpdateForElasticGroupAnnotations keeps the group minimum and the role
// count immutable: every slice of the group's Workload chain is built from them.
func validateUpdateForElasticGroupAnnotations(oldPod, newPod *Pod) field.ErrorList {
	var allErrs field.ErrorList
	for key, path := range map[string]*field.Path{
		podconstants.GroupMinCountAnnotation:  groupMinCountAnnotationPath,
		podconstants.GroupRoleCountAnnotation: groupRoleCountAnnotationPath,
	} {
		oldValue, oldFound := oldPod.pod.Annotations[key]
		newValue, newFound := newPod.pod.Annotations[key]
		if !oldFound && !newFound {
			continue
		}
		allErrs = append(allErrs, validation.ValidateImmutableField(newValue, oldValue, path)...)
	}
	return allErrs
}

// prepareElasticGroupMember stamps what the elastic-job ungater and the topology
// ungater key on: the PodSet label (the role hash), the slice-chain name (the
// group name) and the elastic-job scheduling gate, which keeps a member gated
// until the active slice's granted count has room for it.
func (p *Pod) prepareElasticGroupMember() {
	if p.pod.Labels == nil {
		p.pod.Labels = make(map[string]string, 1)
	}
	p.pod.Labels[constants.PodSetLabel] = string(kueue.NewPodSetReference(p.pod.Annotations[podconstants.RoleHashAnnotation]))
	metav1.SetMetaDataAnnotation(&p.pod.ObjectMeta, kueue.WorkloadSliceNameAnnotation, utilpod.GetPodGroupName(&p.pod))
	utilpod.Gate(&p.pod, kueue.ElasticJobSchedulingGate)
}

func validateTopologyRequest(pod *Pod) field.ErrorList {
	return jobframework.ValidateTASPodSetRequest(metaPath, &pod.pod.ObjectMeta)
}

func validateUpdateForRetriableInGroupAnnotation(oldPod, newPod *Pod) field.ErrorList {
	if groupName := utilpod.GetPodGroupName(&newPod.pod); groupName != "" && isUnretriablePod(oldPod.pod) && !isUnretriablePod(newPod.pod) {
		return field.ErrorList{
			field.Forbidden(retriableInGroupAnnotationPath, "unretriable pod group can't be converted to retriable"),
		}
	}

	return nil
}

func validatePrebuiltWorkloadName(pod *Pod) field.ErrorList {
	var allErrs field.ErrorList

	groupName := utilpod.GetPodGroupName(&pod.pod)
	prebuiltWorkload := jobframework.PrebuiltWorkloadNameFor(&pod.pod)
	if groupName != "" && prebuiltWorkload != "" && prebuiltWorkload != groupName {
		allErrs = append(allErrs, field.Invalid(jobframework.GetPrebuiltWorkloadPath(&pod.pod), prebuiltWorkload, "prebuilt workload and pod group should be equal"))
	}
	return allErrs
}
func validatePodGroupName(obj client.Object) field.ErrorList {
	groupName := obj.GetAnnotations()[podconstants.GroupNameAnnotation]
	if features.Enabled(features.WorkloadIdentifierAnnotations) && groupName != "" {
		return jobframework.ValidateAnnotationAsCRDName(obj, podconstants.GroupNameAnnotation)
	}
	return jobframework.ValidateLabelAsCRDName(obj, podconstants.GroupNameLabel)
}

func getGroupNamePath(obj client.Object) *field.Path {
	groupNameAnnotation := obj.GetAnnotations()[podconstants.GroupNameAnnotation]
	if features.Enabled(features.WorkloadIdentifierAnnotations) && groupNameAnnotation != "" {
		return groupNameAnnotationPath
	}
	return groupNameLabelPath
}
