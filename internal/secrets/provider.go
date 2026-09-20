// Package secrets resolves a shard's credential_ref (e.g.
// "secret/payment/postgresql") into actual database credentials.
//
// Guide section 4.4 is explicit: "Do not store a complete URI containing
// credentials ... in etcd. Keep routing metadata and secrets separate. If
// applications require credentials dynamically, store only a
// credential_ref in etcd and resolve the referenced secret through Vault,
// Kubernetes Secrets, a cloud secret manager, or the organization's
// approved secret-management platform."
//
// This package only defines the seam. The Env provider below is a
// convenient default for local development and tests; production
// deployments should implement Provider against Vault, AWS/GCP Secrets
// Manager, or Kubernetes Secrets and wire it in at startup instead.
package secrets

import (
	"fmt"
	"os"
	"strings"
)

// Credentials is the minimal set of fields needed to open a Postgres
// connection. TLS material, if required, should be handled by the pool
// layer's TLS config, not carried through this struct as plain text.
type Credentials struct {
	Username string
	Password string
}

// Provider resolves a credential_ref into Credentials. Implementations
// must not log the returned password.
type Provider interface {
	Resolve(credentialRef string) (Credentials, error)
}

// EnvProvider resolves a ref like "secret/payment/postgresql" into
// environment variables SECRET_PAYMENT_POSTGRESQL_USER /
// SECRET_PAYMENT_POSTGRESQL_PASSWORD. It exists for local development and
// CI, where a real secret manager isn't available; it is deliberately not
// the default wired into a production main.go.
type EnvProvider struct{}

func NewEnvProvider() *EnvProvider { return &EnvProvider{} }

func (p *EnvProvider) Resolve(credentialRef string) (Credentials, error) {
	key := envKey(credentialRef)
	user := os.Getenv(key + "_USER")
	pass := os.Getenv(key + "_PASSWORD")
	if user == "" {
		return Credentials{}, fmt.Errorf("secrets: no %s_USER set for credential_ref %q", key, credentialRef)
	}
	return Credentials{Username: user, Password: pass}, nil
}

func envKey(ref string) string {
	replacer := strings.NewReplacer("/", "_", "-", "_", ".", "_")
	return strings.ToUpper(replacer.Replace(ref))
}
