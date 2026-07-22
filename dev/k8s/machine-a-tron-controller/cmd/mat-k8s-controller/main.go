// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// mat-k8s-controller is a Kubernetes controller that reconciles Services
// for machine-a-tron mock BMC endpoints.
//
// It polls the machine-a-tron /machines/status API and creates/updates/deletes
// Kubernetes Services to expose Redfish (and optionally IPMI) endpoints for
// each mock BMC.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/NVIDIA/infra-controller/dev/k8s/machine-a-tron-controller/pkg/controller"
	"github.com/NVIDIA/infra-controller/dev/k8s/machine-a-tron-controller/pkg/matclient"
)

func main() {
	// Flags
	matURL := flag.String("mat-url", envOrDefault("MAT_URL", "https://nico-machine-a-tron-bmc-mock:1266"),
		"Machine-a-tron base URL")
	namespace := flag.String("namespace", envOrDefault("NAMESPACE", "nico-system"),
		"Kubernetes namespace for Services")
	syncInterval := flag.Duration("sync-interval", parseDurationOrDefault("SYNC_INTERVAL", 30*time.Second),
		"Interval between reconciliation passes")
	kubeconfig := flag.String("kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to kubeconfig (uses in-cluster config if empty)")
	targetSelector := flag.String("target-selector", envOrDefault("TARGET_SELECTOR", "app.kubernetes.io/name=nico-machine-a-tron"),
		"Pod selector for Services (comma-separated key=value pairs)")
	clusterIPPrefix := flag.String("cluster-ip-prefix", os.Getenv("CLUSTER_IP_PREFIX"),
		"Prefix for static ClusterIP assignment (e.g., 10.96)")
	insecureSkipVerify := flag.Bool("insecure-skip-verify", envBoolOrDefault("INSECURE_SKIP_VERIFY", true),
		"Skip TLS certificate verification (for self-signed certs)")
	logLevel := flag.String("log-level", envOrDefault("LOG_LEVEL", "info"),
		"Log level (debug, info, warn, error)")

	flag.Parse()

	// Setup logger
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(level).
		With().
		Timestamp().
		Str("component", "mat-k8s-controller").
		Logger()

	logger.Info().
		Str("mat_url", *matURL).
		Str("namespace", *namespace).
		Dur("sync_interval", *syncInterval).
		Str("target_selector", *targetSelector).
		Str("cluster_ip_prefix", *clusterIPPrefix).
		Bool("insecure_skip_verify", *insecureSkipVerify).
		Msg("starting controller")

	// Create machine-a-tron client
	clientOpts := []matclient.Option{matclient.WithLogger(logger)}
	if *insecureSkipVerify {
		clientOpts = append(clientOpts, matclient.WithInsecureSkipVerify())
	}

	matClient, err := matclient.NewClient(*matURL, clientOpts...)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create machine-a-tron client")
	}

	// Create Kubernetes client
	var k8sConfig *rest.Config
	if *kubeconfig != "" {
		k8sConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		k8sConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create Kubernetes config")
	}

	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create Kubernetes clientset")
	}

	// Parse target selector
	selector := parseSelector(*targetSelector)

	// Create service builder
	builder := &controller.ServiceBuilder{
		Namespace:       *namespace,
		TargetSelector:  selector,
		ClusterIPPrefix: *clusterIPPrefix,
	}

	// Create reconciler
	k8sClient := controller.NewRealK8sServiceClient(clientset)
	reconciler := controller.NewReconciler(matClient, builder, k8sClient)

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info().Str("signal", sig.String()).Msg("received shutdown signal")
		cancel()
	}()

	// Run reconciliation loop
	ticker := time.NewTicker(*syncInterval)
	defer ticker.Stop()

	// Run initial reconciliation
	runReconcile(ctx, reconciler, logger)

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("shutting down")
			return
		case <-ticker.C:
			runReconcile(ctx, reconciler, logger)
		}
	}
}

func runReconcile(ctx context.Context, r *controller.Reconciler, logger zerolog.Logger) {
	start := time.Now()
	result := r.Reconcile(ctx)
	elapsed := time.Since(start)

	logEvent := logger.Info().
		Int("created", result.Created).
		Int("updated", result.Updated).
		Int("deleted", result.Deleted).
		Dur("elapsed", elapsed)

	if len(result.Errors) > 0 {
		logEvent = logger.Error().
			Int("created", result.Created).
			Int("updated", result.Updated).
			Int("deleted", result.Deleted).
			Int("errors", len(result.Errors)).
			Dur("elapsed", elapsed)

		for _, err := range result.Errors {
			logger.Error().Err(err).Msg("reconciliation error")
		}
	}

	logEvent.Msg("reconciliation complete")
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func envBoolOrDefault(key string, defaultValue bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultValue
}

func parseDurationOrDefault(envKey string, defaultValue time.Duration) time.Duration {
	if v := os.Getenv(envKey); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultValue
}

func parseSelector(s string) map[string]string {
	result := make(map[string]string)
	if s == "" {
		return result
	}

	// Simple parser for key=value,key2=value2 format
	pairs := splitPairs(s, ',')
	for _, pair := range pairs {
		kv := splitPairs(pair, '=')
		if len(kv) == 2 {
			result[kv[0]] = kv[1]
		}
	}
	return result
}

func splitPairs(s string, sep rune) []string {
	var result []string
	var current string
	for _, r := range s {
		if r == sep {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(r)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
