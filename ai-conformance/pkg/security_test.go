package pkg

import (
	"context"
	"testing"

	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// How we might test it: Deploy a Pod to a node with available accelerators, without requesting
// accelerator resources in the Pod spec. Execute a command in the Pod to probe for accelerator
// devices, and the command should fail or report that no accelerator devices are found. Create
// two Pods, each is allocated an accelerator resource. Execute a command in one Pod to attempt
// to access the other Pod’s accelerator, and should be denied.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestSecureAcceleratorAccess(t *testing.T) {
	description := "Ensure that access to accelerators from within containers is properly isolated and mediated by " +
		"the Kubernetes resource management framework (device plugin or DRA) and container runtime, preventing unauthorized access or interference between workloads."

	f := features.New("secure_accelerator_access").
		WithLabel("type", "security").
		WithLabel("id", "secure_accelerator_access").
		WithLabel("level", "MUST").
		AssessWithDescription("secure_accelerator_access", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			return ctx
		})

	testenv.Test(t, f.Feature())
}
