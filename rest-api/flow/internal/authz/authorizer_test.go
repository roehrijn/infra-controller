// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package authz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

const (
	allowedIdentity = "spiffe://example.test/ns/site/sa/site-workflow"
	deniedIdentity  = "spiffe://example.test/ns/site/sa/unrelated-service"
)

func TestConfigValidate(t *testing.T) {
	tests := map[string]struct {
		identities []string
		wantErr    string
	}{
		"valid": {
			identities: []string{allowedIdentity},
		},
		"empty": {
			wantErr: "at least one allowed service identity is required",
		},
		"audit may use an empty allowlist": {
			identities: nil,
		},
		"duplicate": {
			identities: []string{allowedIdentity, allowedIdentity},
			wantErr:    "duplicate allowed service identity",
		},
		"wrong scheme": {
			identities: []string{"https://example.test/workload"},
			wantErr:    "not a valid SPIFFE ID",
		},
		"missing trust domain": {
			identities: []string{"spiffe:///ns/site/sa/site-workflow"},
			wantErr:    "not a valid SPIFFE ID",
		},
		"missing workload path": {
			identities: []string{"spiffe://example.test"},
			wantErr:    "must include a workload path",
		},
		"wildcard": {
			identities: []string{"spiffe://example.test/ns/site/sa/*"},
			wantErr:    "not a valid SPIFFE ID",
		},
		"query": {
			identities: []string{"spiffe://example.test/workload?role=admin"},
			wantErr:    "not a valid SPIFFE ID",
		},
		"fragment": {
			identities: []string{"spiffe://example.test/workload#admin"},
			wantErr:    "not a valid SPIFFE ID",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mode := ModeEnforce
			if name == "audit may use an empty allowlist" {
				mode = ModeAudit
			}
			err := (Config{AllowedServiceIdentities: test.identities, Mode: mode}).Validate()
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestConfigValidateMode(t *testing.T) {
	require.ErrorContains(t, (Config{Mode: "audit"}).Validate(), "authorization mode")
	require.NoError(t, (Config{Mode: ModeAudit}).Validate())
	require.ErrorContains(t, (Config{}).Validate(), "authorization mode")
}

func TestNewMode(t *testing.T) {
	tests := map[string]struct {
		value   string
		want    Mode
		wantErr string
	}{
		"undefined": {
			want: DefaultMode,
		},
		"whitespace uses default": {
			value: "   ",
			want:  DefaultMode,
		},
		"audit": {
			value: string(ModeAudit),
			want:  ModeAudit,
		},
		"audit with whitespace": {
			value: "  " + string(ModeAudit) + "  ",
			want:  ModeAudit,
		},
		"enforce": {
			value: string(ModeEnforce),
			want:  ModeEnforce,
		},
		"invalid": {
			value:   "audit",
			want:    ModeUndefined,
			wantErr: "authorization mode",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mode, err := NewMode(test.value)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.want, mode)
		})
	}
}

func TestAuthorizerAuthorize(t *testing.T) {
	authorizer, err := New(Config{
		AllowedServiceIdentities: []string{allowedIdentity},
		Mode:                     DefaultMode,
	})
	require.NoError(t, err)

	tests := map[string]struct {
		context       func() context.Context
		wantRejection rejection
		wantID        string
		wantMessage   bool
	}{
		"missing peer": {
			context:       context.Background,
			wantRejection: rejectionUnauthenticated,
			wantMessage:   true,
		},
		"non-TLS peer": {
			context: func() context.Context {
				return peer.NewContext(context.Background(), &peer.Peer{
					AuthInfo: testAuthInfo{},
				})
			},
			wantRejection: rejectionUnauthenticated,
			wantMessage:   true,
		},
		"missing verified chain": {
			context: func() context.Context {
				return tlsPeerContext(nil)
			},
			wantRejection: rejectionUnauthenticated,
			wantMessage:   true,
		},
		"missing SPIFFE identity": {
			context: func() context.Context {
				return tlsPeerContext(&x509.Certificate{})
			},
			wantRejection: rejectionUnauthenticated,
			wantMessage:   true,
		},
		"pathless SPIFFE identity is not allowed": {
			context: func() context.Context {
				return tlsPeerContext(certificateWithURIs(t, "spiffe://example.test"))
			},
			wantRejection: rejectionPermissionDenied,
			wantID:        "spiffe://example.test",
			wantMessage:   true,
		},
		"ambiguous SPIFFE identity": {
			context: func() context.Context {
				return tlsPeerContext(certificateWithURIs(t, allowedIdentity, deniedIdentity))
			},
			wantRejection: rejectionUnauthenticated,
			wantMessage:   true,
		},
		"identity is not allowed": {
			context: func() context.Context {
				return tlsPeerContext(certificateWithURIs(t, deniedIdentity))
			},
			wantRejection: rejectionPermissionDenied,
			wantID:        deniedIdentity,
			wantMessage:   true,
		},
		"identity is allowed": {
			context: func() context.Context {
				return tlsPeerContext(certificateWithURIs(t, allowedIdentity))
			},
			wantID: allowedIdentity,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			decision := authorizer.authorize(test.context())
			assert.Equal(t, test.wantRejection, decision.rejection)
			assert.Equal(t, test.wantID, decision.identity)
			assert.Equal(t, test.wantMessage, decision.message != "")
		})
	}
}

type testAuthInfo struct{}

func (testAuthInfo) AuthType() string {
	return "test"
}

func tlsPeerContext(certificate *x509.Certificate) context.Context {
	state := tls.ConnectionState{}
	if certificate != nil {
		state.PeerCertificates = []*x509.Certificate{certificate}
		state.VerifiedChains = [][]*x509.Certificate{{certificate}}
	}

	return peer.NewContext(context.Background(), &peer.Peer{
		Addr:     &net.IPAddr{},
		AuthInfo: credentials.TLSInfo{State: state},
	})
}

func certificateWithURIs(t *testing.T, identities ...string) *x509.Certificate {
	t.Helper()

	certificate := &x509.Certificate{}
	for _, identity := range identities {
		uri, err := url.Parse(identity)
		require.NoError(t, err)
		certificate.URIs = append(certificate.URIs, uri)
	}
	return certificate
}
