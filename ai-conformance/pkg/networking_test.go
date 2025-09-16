package pkg

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// How we might test it: Verify that all the gateway.networking.k8s.io/v1 Gateway API resources are enabled.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestAiInference(t *testing.T) {
	description := "Support the Kubernetes Gateway API with an implementation for advanced traffic management for inference services, " +
		"which enables capabilities like weighted traffic splitting, header-based routing (for OpenAI protocol headers), and optional integration with service meshes."

	f := features.New("ai_inference").
		WithLabel("type", "networking").
		WithLabel("id", "ai_inference").
		WithLabel("level", "MUST").
		AssessWithDescription("Verify that all the gateway.networking.k8s.io/v1 Gateway API resources are enabled.", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			gvks := []schema.GroupVersionKind{
				{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "GatewayClass"},
				{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "Gateway"},
				{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "GRPCRoute"},
				{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"},
				{Group: "gateway.networking.k8s.io", Version: "v1beta1", Kind: "ReferenceGrant"},
			}

			for _, gvk := range gvks {
				found, err := IsCrdAvailable(cfg.Client().RESTConfig(), gvk)
				if err != nil {
					t.Errorf("Failed to check %s: %v", gvk, err)
					return ctx
				}
				if !found {
					t.Errorf("missing %s", gvk)
					return ctx
				}
			}
			t.Logf("found all the gateway.networking.k8s.io/v1 Gateway API resources: %v", gvks)
			return ctx
		})

	testenv.Test(t, f.Feature())
}
