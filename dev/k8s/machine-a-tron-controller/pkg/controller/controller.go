// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package controller implements the Kubernetes Service reconciliation logic
// for machine-a-tron mock BMC endpoints.
package controller

import (
	"context"
	"fmt"
	"strconv"

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
	// TargetSelector is the pod selector that Services should target.
	// This should match the machine-a-tron pod labels.
	TargetSelector map[string]string
	// ClusterIPPrefix is a prefix for static ClusterIP assignment.
	// If set along with BMC IP, Services get predictable IPs.
	// Format: "10.96.{last-two-octets-of-bmc-ip}"
	ClusterIPPrefix string
}

// BuildServiceName generates a consistent service name for a machine.
func BuildServiceName(machineType, matID string) string {
	// Use first 8 chars of mat-id for reasonable length
	shortID := matID
	if len(matID) > 8 {
		shortID = matID[:8]
	}
	return fmt.Sprintf("mat-bmc-%s-%s", machineType, shortID)
}

// BuildService creates a Kubernetes Service for a machine's BMC.
func (b *ServiceBuilder) BuildService(machine *matclient.MachineStatus, machineType, parentMatID string) *corev1.Service {
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

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   b.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: b.TargetSelector,
			Ports:    ports,
		},
	}

	// Set static ClusterIP if configured and BMC IP is known
	if b.ClusterIPPrefix != "" && machine.BMC.IP != nil {
		clusterIP := buildStaticClusterIP(b.ClusterIPPrefix, *machine.BMC.IP)
		if clusterIP != "" {
			svc.Spec.ClusterIP = clusterIP
		}
	}

	return svc
}

// BuildServicesFromStatus generates all Services from a machines status response.
func (b *ServiceBuilder) BuildServicesFromStatus(status *matclient.MachinesStatusResponse) []*corev1.Service {
	var services []*corev1.Service

	for i := range status.Machines {
		machine := &status.Machines[i]

		// Build service for the host BMC
		svc := b.BuildService(machine, MachineTypeHost, "")
		services = append(services, svc)

		// Build services for DPU BMCs
		for j := range machine.DPUs {
			dpu := &machine.DPUs[j]
			dpuSvc := b.BuildService(dpu, MachineTypeDPU, machine.MatID)
			services = append(services, dpuSvc)
		}
	}

	return services
}

// buildStaticClusterIP constructs a ClusterIP from a prefix and BMC IP.
// For example, with prefix "10.96" and bmcIP "172.20.0.20",
// it produces "10.96.0.20" (uses last two octets of BMC IP).
func buildStaticClusterIP(prefix, bmcIP string) string {
	// Parse last two octets from BMC IP
	// This is a simple implementation; production might need more robust parsing
	var a, b, c, d int
	n, err := fmt.Sscanf(bmcIP, "%d.%d.%d.%d", &a, &b, &c, &d)
	if err != nil || n != 4 {
		return ""
	}

	return fmt.Sprintf("%s.%d.%d", prefix, c, d)
}

// ServiceDiff represents changes between desired and existing services.
type ServiceDiff struct {
	Create []*corev1.Service
	Update []*corev1.Service
	Delete []string // service names to delete
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
			// Copy ResourceVersion for update
			svc.ResourceVersion = existingSvc.ResourceVersion
			// Preserve ClusterIP if not explicitly set
			if svc.Spec.ClusterIP == "" {
				svc.Spec.ClusterIP = existingSvc.Spec.ClusterIP
			}
			diff.Update = append(diff.Update, svc)
		}
	}

	// Find services to delete (exist but not in desired)
	for name, svc := range existingByName {
		if !desiredNames[name] && isManagedByController(svc) {
			diff.Delete = append(diff.Delete, name)
		}
	}

	return diff
}

// needsUpdate checks if a service needs to be updated.
func needsUpdate(desired, existing *corev1.Service) bool {
	// Check port changes
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

	// Check label changes (except for managed-by which should always be set)
	for k, v := range desired.Labels {
		if existing.Labels[k] != v {
			return true
		}
	}

	// Check annotation changes
	for k, v := range desired.Annotations {
		if existing.Annotations[k] != v {
			return true
		}
	}

	return false
}

// isManagedByController checks if a service is managed by this controller.
func isManagedByController(svc *corev1.Service) bool {
	return svc.Labels[LabelManagedBy] == LabelManagedByValue
}

// Reconciler handles the reconciliation loop.
type Reconciler struct {
	matClient      *matclient.Client
	serviceBuilder *ServiceBuilder
	k8sClient      K8sServiceClient
}

// K8sServiceClient is an interface for Kubernetes Service operations.
type K8sServiceClient interface {
	List(ctx context.Context, namespace string, labelSelector string) ([]*corev1.Service, error)
	Create(ctx context.Context, svc *corev1.Service) error
	Update(ctx context.Context, svc *corev1.Service) error
	Delete(ctx context.Context, namespace, name string) error
}

// NewReconciler creates a new Reconciler.
func NewReconciler(matClient *matclient.Client, builder *ServiceBuilder, k8sClient K8sServiceClient) *Reconciler {
	return &Reconciler{
		matClient:      matClient,
		serviceBuilder: builder,
		k8sClient:      k8sClient,
	}
}

// ReconcileResult contains the result of a reconciliation.
type ReconcileResult struct {
	Created int
	Updated int
	Deleted int
	Errors  []error
}

// Reconcile performs a single reconciliation pass.
func (r *Reconciler) Reconcile(ctx context.Context) ReconcileResult {
	result := ReconcileResult{}

	// Fetch current machine status
	status, err := r.matClient.GetMachinesStatus(ctx)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("fetching machine status: %w", err))
		return result
	}

	// Build desired services
	desired := r.serviceBuilder.BuildServicesFromStatus(status)

	// List existing managed services
	selector := fmt.Sprintf("%s=%s", LabelManagedBy, LabelManagedByValue)
	existing, err := r.k8sClient.List(ctx, r.serviceBuilder.Namespace, selector)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("listing existing services: %w", err))
		return result
	}

	// Compute diff
	diff := ComputeServiceDiff(desired, existing)

	// Apply creates
	for _, svc := range diff.Create {
		if err := r.k8sClient.Create(ctx, svc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("creating service %s: %w", svc.Name, err))
		} else {
			result.Created++
		}
	}

	// Apply updates
	for _, svc := range diff.Update {
		if err := r.k8sClient.Update(ctx, svc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("updating service %s: %w", svc.Name, err))
		} else {
			result.Updated++
		}
	}

	// Apply deletes
	for _, name := range diff.Delete {
		if err := r.k8sClient.Delete(ctx, r.serviceBuilder.Namespace, name); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("deleting service %s: %w", name, err))
		} else {
			result.Deleted++
		}
	}

	return result
}
