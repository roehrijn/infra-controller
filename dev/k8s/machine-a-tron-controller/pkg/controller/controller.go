// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package controller implements the Kubernetes Service reconciliation logic
// for machine-a-tron mock BMC endpoints.
package controller

import (
	"context"
	"fmt"
	"strconv"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/NVIDIA/infra-controller/dev/k8s/machine-a-tron-controller/pkg/matclient"
)

const (
	// LabelManagedBy identifies the controller managing the resource.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// LabelManagedByValue is the value for the managed-by label.
	LabelManagedByValue = "mat-k8s-controller"

	// LabelPodName is the label that identifies which machine-a-tron pod owns this service.
	LabelPodName = "nvidia-infra-controller/pod-name"

	// LabelMatID is the machine-a-tron ID label.
	LabelMatID = "machine-a-tron.nvidia.com/mat-id"
	// LabelMachineID is the NICo machine ID label.
	LabelMachineID = "machine-a-tron.nvidia.com/machine-id"
	// LabelMachineType indicates if this is a host or DPU.
	LabelMachineType = "machine-a-tron.nvidia.com/machine-type"
	// LabelParentMatID is the parent host's mat-id for DPU services.
	LabelParentMatID = "machine-a-tron.nvidia.com/parent-mat-id"

	// AnnotationBMCIP stores the BMC IP address.
	AnnotationBMCIP = "machine-a-tron.nvidia.com/bmc-ip"
	// AnnotationAPIState stores the machine's API state.
	AnnotationAPIState = "machine-a-tron.nvidia.com/api-state"
	// AnnotationPowerState stores the machine's power state.
	AnnotationPowerState = "machine-a-tron.nvidia.com/power-state"
	// AnnotationHardwareType stores the hardware type.
	AnnotationHardwareType = "machine-a-tron.nvidia.com/hardware-type"
	// AnnotationRedfishListenPort stores the internal Redfish listen port.
	AnnotationRedfishListenPort = "machine-a-tron.nvidia.com/redfish-listen-port"
	// AnnotationIPMIListenPort stores the internal IPMI listen port.
	AnnotationIPMIListenPort = "machine-a-tron.nvidia.com/ipmi-listen-port"

	// MachineTypeHost indicates a host machine.
	MachineTypeHost = "host"
	// MachineTypeDPU indicates a DPU.
	MachineTypeDPU = "dpu"

	// PortNameRedfish is the name of the Redfish port in the Service.
	PortNameRedfish = "redfish"
	// PortNameIPMI is the name of the IPMI port in the Service.
	PortNameIPMI = "ipmi"
)

// ServiceBuilder builds Kubernetes Services from machine status.
type ServiceBuilder struct {
	// Namespace is the target namespace for Services.
	Namespace string
	// BaseSelector is the base pod selector (e.g., app.kubernetes.io/name=nico-machine-a-tron).
	BaseSelector map[string]string
}

// BuildServiceName generates a consistent service name for a machine.
func BuildServiceName(machineType, matID string) string {
	shortID := matID
	if len(matID) > 8 {
		shortID = matID[:8]
	}
	return fmt.Sprintf("mat-bmc-%s-%s", machineType, shortID)
}

// BuildService creates a Kubernetes Service for a machine's BMC.
// podName is used to create a pod-specific selector for multi-pod deployments.
func (b *ServiceBuilder) BuildService(machine *matclient.MachineStatus, machineType, parentMatID, podName string) *corev1.Service {
	name := BuildServiceName(machineType, machine.MatID)

	labels := map[string]string{
		LabelManagedBy:   LabelManagedByValue,
		LabelMatID:       machine.MatID,
		LabelMachineType: machineType,
	}

	annotations := map[string]string{
		AnnotationAPIState:          machine.APIState,
		AnnotationPowerState:        machine.PowerState,
		AnnotationRedfishListenPort: strconv.Itoa(int(machine.BMC.Redfish.ListenPort)),
	}

	if machine.MachineID != nil {
		labels[LabelMachineID] = *machine.MachineID
	}
	if parentMatID != "" {
		labels[LabelParentMatID] = parentMatID
	}
	if podName != "" {
		labels[LabelPodName] = podName
	}
	if machine.BMC.IP != nil {
		annotations[AnnotationBMCIP] = *machine.BMC.IP
	}
	if machine.HardwareType != nil {
		annotations[AnnotationHardwareType] = *machine.HardwareType
	}
	if machine.BMC.IPMI != nil {
		annotations[AnnotationIPMIListenPort] = strconv.Itoa(int(machine.BMC.IPMI.ListenPort))
	}

	ports := []corev1.ServicePort{
		{
			Name:       PortNameRedfish,
			Protocol:   corev1.ProtocolTCP,
			Port:       int32(machine.BMC.Redfish.ReachablePort),
			TargetPort: intstr.FromInt32(int32(machine.BMC.Redfish.ListenPort)),
		},
	}

	if machine.BMC.IPMI != nil {
		ports = append(ports, corev1.ServicePort{
			Name:       PortNameIPMI,
			Protocol:   corev1.ProtocolUDP,
			Port:       int32(machine.BMC.IPMI.ReachablePort),
			TargetPort: intstr.FromInt32(int32(machine.BMC.IPMI.ListenPort)),
		})
	}

	// Build selector: base selector + pod-specific selector for multi-pod
	selector := make(map[string]string)
	for k, v := range b.BaseSelector {
		selector[k] = v
	}
	if podName != "" {
		selector[LabelPodName] = podName
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   b.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports:    ports,
		},
	}

	// Use BMC IP directly as ClusterIP
	if machine.BMC.IP != nil {
		svc.Spec.ClusterIP = *machine.BMC.IP
	}

	return svc
}

// BuildServicesFromStatus generates all Services from a machines status response.
// podName identifies which machine-a-tron pod these machines belong to.
func (b *ServiceBuilder) BuildServicesFromStatus(status *matclient.MachinesStatusResponse, podName string) []*corev1.Service {
	var services []*corev1.Service

	for i := range status.Machines {
		machine := &status.Machines[i]
		svc := b.BuildService(machine, MachineTypeHost, "", podName)
		services = append(services, svc)

		for j := range machine.DPUs {
			dpu := &machine.DPUs[j]
			dpuSvc := b.BuildService(dpu, MachineTypeDPU, machine.MatID, podName)
			services = append(services, dpuSvc)
		}
	}

	return services
}

// ServiceDiff represents changes between desired and existing services.
type ServiceDiff struct {
	Create []*corev1.Service
	Update []*corev1.Service
	Delete []string
}

// ComputeServiceDiff calculates what changes need to be made.
func ComputeServiceDiff(desired []*corev1.Service, existing []*corev1.Service) ServiceDiff {
	var diff ServiceDiff

	existingByName := make(map[string]*corev1.Service)
	for _, svc := range existing {
		existingByName[svc.Name] = svc
	}

	desiredNames := make(map[string]bool)
	for _, svc := range desired {
		desiredNames[svc.Name] = true

		existingSvc, exists := existingByName[svc.Name]
		if !exists {
			diff.Create = append(diff.Create, svc)
			continue
		}

		if needsUpdate(svc, existingSvc) {
			svc.ResourceVersion = existingSvc.ResourceVersion
			if svc.Spec.ClusterIP == "" {
				svc.Spec.ClusterIP = existingSvc.Spec.ClusterIP
			}
			diff.Update = append(diff.Update, svc)
		}
	}

	for name, svc := range existingByName {
		if !desiredNames[name] && isManagedByController(svc) {
			diff.Delete = append(diff.Delete, name)
		}
	}

	return diff
}

func needsUpdate(desired, existing *corev1.Service) bool {
	if len(desired.Spec.Ports) != len(existing.Spec.Ports) {
		return true
	}

	existingPorts := make(map[string]corev1.ServicePort)
	for _, p := range existing.Spec.Ports {
		existingPorts[p.Name] = p
	}

	for _, dp := range desired.Spec.Ports {
		ep, ok := existingPorts[dp.Name]
		if !ok {
			return true
		}
		if dp.Port != ep.Port || dp.TargetPort != ep.TargetPort || dp.Protocol != ep.Protocol {
			return true
		}
	}

	for k, v := range desired.Labels {
		if existing.Labels[k] != v {
			return true
		}
	}

	for k, v := range desired.Annotations {
		if existing.Annotations[k] != v {
			return true
		}
	}

	// Check selector changes (important for multi-pod)
	for k, v := range desired.Spec.Selector {
		if existing.Spec.Selector[k] != v {
			return true
		}
	}

	return false
}

func isManagedByController(svc *corev1.Service) bool {
	return svc.Labels[LabelManagedBy] == LabelManagedByValue
}

// K8sServiceClient is an interface for Kubernetes Service operations.
type K8sServiceClient interface {
	List(ctx context.Context, namespace string, labelSelector string) ([]*corev1.Service, error)
	Create(ctx context.Context, svc *corev1.Service) error
	Update(ctx context.Context, svc *corev1.Service) error
	Delete(ctx context.Context, namespace, name string) error
}

// ReconcileResult contains the result of a reconciliation.
type ReconcileResult struct {
	Created int
	Updated int
	Deleted int
	Errors  []error
}

// Reconciler discovers machine-a-tron pods and reconciles Services.
type Reconciler struct {
	discovery      *MatPodDiscovery
	serviceBuilder *ServiceBuilder
	k8sClient      K8sServiceClient
	clientOpts     []matclient.Option
	logger         zerolog.Logger
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	discovery *MatPodDiscovery,
	builder *ServiceBuilder,
	k8sClient K8sServiceClient,
	clientOpts []matclient.Option,
	logger zerolog.Logger,
) *Reconciler {
	return &Reconciler{
		discovery:      discovery,
		serviceBuilder: builder,
		k8sClient:      k8sClient,
		clientOpts:     clientOpts,
		logger:         logger,
	}
}

// Reconcile performs a reconciliation pass across all discovered machine-a-tron instances.
func (r *Reconciler) Reconcile(ctx context.Context) ReconcileResult {
	result := ReconcileResult{}

	// Discover machine-a-tron instances
	instances, err := r.discovery.Discover(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("discovering machine-a-tron instances: %w", err))
		return result
	}

	if len(instances) == 0 {
		r.logger.Warn().Msg("no machine-a-tron instances discovered")
		return result
	}

	r.logger.Debug().Int("count", len(instances)).Msg("discovered machine-a-tron instances")

	// Collect all machines from all instances
	var allDesired []*corev1.Service

	for _, instance := range instances {
		client, err := matclient.NewClient(instance.URL, r.clientOpts...)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("creating client for %s: %w", instance.URL, err))
			continue
		}

		status, err := client.GetMachinesStatus(ctx)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("fetching status from %s: %w", instance.URL, err))
			continue
		}

		services := r.serviceBuilder.BuildServicesFromStatus(status, instance.PodName)
		r.logger.Debug().
			Str("url", instance.URL).
			Str("pod", instance.PodName).
			Int("machines", len(services)).
			Msg("fetched machines from instance")
		allDesired = append(allDesired, services...)
	}

	// List existing managed services
	selector := fmt.Sprintf("%s=%s", LabelManagedBy, LabelManagedByValue)
	existing, err := r.k8sClient.List(ctx, r.serviceBuilder.Namespace, selector)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("listing existing services: %w", err))
		return result
	}

	// Compute and apply diff
	diff := ComputeServiceDiff(allDesired, existing)

	for _, svc := range diff.Create {
		if err := r.k8sClient.Create(ctx, svc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("creating service %s: %w", svc.Name, err))
		} else {
			result.Created++
		}
	}

	for _, svc := range diff.Update {
		if err := r.k8sClient.Update(ctx, svc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("updating service %s: %w", svc.Name, err))
		} else {
			result.Updated++
		}
	}

	for _, name := range diff.Delete {
		if err := r.k8sClient.Delete(ctx, r.serviceBuilder.Namespace, name); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("deleting service %s: %w", name, err))
		} else {
			result.Deleted++
		}
	}

	return result
}
