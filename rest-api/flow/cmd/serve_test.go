// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/infra-controller/rest-api/flow/internal/authz"
	cmconfig "github.com/NVIDIA/infra-controller/rest-api/flow/internal/task/componentmanager/config"
	"github.com/NVIDIA/infra-controller/rest-api/flow/pkg/common/devicetypes"
)

// TestApplyComputeImplementationOverride covers the env-var fallback
// path that exists for migrating compute between nicolegacy and the new
// Component Manager-based nico implementation. Subsequent catalog
// validation rejects unknown names, so the override here is intentionally
// minimal: it only adjusts the config map.
func TestApplyComputeImplementationOverride(t *testing.T) {
	t.Run("env unset is a no-op", func(t *testing.T) {
		t.Setenv(computeImplEnvVar, "")

		cfg := cmconfig.Config{
			ComponentManagers: map[devicetypes.ComponentType]string{
				devicetypes.ComponentTypeCompute: "nicolegacy",
			},
		}

		applyComputeImplementationOverride(&cfg)

		assert.Equal(t, "nicolegacy", cfg.ComponentManagers[devicetypes.ComponentTypeCompute])
	})

	t.Run("whitespace value is treated as unset", func(t *testing.T) {
		t.Setenv(computeImplEnvVar, "   ")

		cfg := cmconfig.Config{
			ComponentManagers: map[devicetypes.ComponentType]string{
				devicetypes.ComponentTypeCompute: "nicolegacy",
			},
		}

		applyComputeImplementationOverride(&cfg)

		assert.Equal(t, "nicolegacy", cfg.ComponentManagers[devicetypes.ComponentTypeCompute])
	})

	t.Run("override replaces existing compute selection", func(t *testing.T) {
		t.Setenv(computeImplEnvVar, "nico")

		cfg := cmconfig.Config{
			ComponentManagers: map[devicetypes.ComponentType]string{
				devicetypes.ComponentTypeCompute:    "nicolegacy",
				devicetypes.ComponentTypeNVSwitch:   "nico",
				devicetypes.ComponentTypePowerShelf: "nico",
			},
		}

		applyComputeImplementationOverride(&cfg)

		assert.Equal(t, "nico", cfg.ComponentManagers[devicetypes.ComponentTypeCompute])
		// Other component types must be untouched.
		assert.Equal(t, "nico", cfg.ComponentManagers[devicetypes.ComponentTypeNVSwitch])
		assert.Equal(t, "nico", cfg.ComponentManagers[devicetypes.ComponentTypePowerShelf])
	})

	t.Run("override surrounding whitespace is trimmed", func(t *testing.T) {
		t.Setenv(computeImplEnvVar, "  nico  ")

		cfg := cmconfig.Config{
			ComponentManagers: map[devicetypes.ComponentType]string{
				devicetypes.ComponentTypeCompute: "nicolegacy",
			},
		}

		applyComputeImplementationOverride(&cfg)

		assert.Equal(t, "nico", cfg.ComponentManagers[devicetypes.ComponentTypeCompute])
	})

	t.Run("override initialises map when nil", func(t *testing.T) {
		t.Setenv(computeImplEnvVar, "nico")

		cfg := cmconfig.Config{}

		applyComputeImplementationOverride(&cfg)

		assert.Equal(t, "nico", cfg.ComponentManagers[devicetypes.ComponentTypeCompute])
	})
}

func TestLoadAuthorizationConfig(t *testing.T) {
	originalIdentities := allowedServiceIdentities
	t.Cleanup(func() {
		allowedServiceIdentities = originalIdentities
	})

	t.Run("uses CLI identities when file environment variable is unset", func(t *testing.T) {
		allowedServiceIdentities = []string{allowedServiceIdentityForTest}
		t.Setenv(allowedServiceIdentitiesFileEnvVar, "temporary")
		require.NoError(t, os.Unsetenv(allowedServiceIdentitiesFileEnvVar))

		config, err := loadAuthorizationConfig()

		require.NoError(t, err)
		assert.Equal(t, []string{allowedServiceIdentityForTest}, config.AllowedServiceIdentities)
	})

	t.Run("rejects blank file environment variable", func(t *testing.T) {
		allowedServiceIdentities = nil
		t.Setenv(allowedServiceIdentitiesFileEnvVar, "   ")

		_, err := loadAuthorizationConfig()

		require.ErrorContains(t, err, "read allowed service identities file \"\"")
	})

	t.Run("loads plain-text identity list and audit mode", func(t *testing.T) {
		allowedServiceIdentities = nil
		path := filepath.Join(t.TempDir(), "allowed-services.txt")
		content := "\n  " + allowedServiceIdentityForTest + "  \n\n"
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		t.Setenv(allowedServiceIdentitiesFileEnvVar, path)
		t.Setenv(authorizationModeEnvVar, string(authz.ModeAudit))

		config, err := loadAuthorizationConfig()

		require.NoError(t, err)
		assert.Equal(t, []string{allowedServiceIdentityForTest}, config.AllowedServiceIdentities)
		assert.Equal(t, authz.ModeAudit, config.Mode)
	})

	t.Run("rejects file and CLI identities together", func(t *testing.T) {
		allowedServiceIdentities = []string{allowedServiceIdentityForTest}
		t.Setenv(allowedServiceIdentitiesFileEnvVar, "identities.txt")

		_, err := loadAuthorizationConfig()

		require.ErrorContains(t, err, "cannot be configured by both file and command-line options")
	})

	t.Run("does not interpret comments", func(t *testing.T) {
		allowedServiceIdentities = nil
		path := filepath.Join(t.TempDir(), "allowed-services.txt")
		require.NoError(t, os.WriteFile(path, []byte("# service identities\n"), 0o600))
		t.Setenv(allowedServiceIdentitiesFileEnvVar, path)

		config, err := loadAuthorizationConfig()

		require.NoError(t, err)
		require.ErrorContains(t, config.Validate(), "not a valid SPIFFE ID")
	})
}

const allowedServiceIdentityForTest = "spiffe://example.test/ns/site/sa/site-workflow"
