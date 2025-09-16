package pkg

import (
	"context"
	"testing"

	"k8s.io/client-go/discovery"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// How we might test it: Verify that all the resource.k8s.io/v1 DRA API resources are enabled.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestDraSupport(t *testing.T) {
	description := "Support Dynamic Resource Allocation (DRA) APIs to enable more flexible and fine-grained resource requests beyond simple counts."

	f := features.New("dra_support").
		WithLabel("type", "accelerators").
		WithLabel("id", "dra_support").
		WithLabel("level", "MUST").
		AssessWithDescription("Verify that all the resource.k8s.io/v1 DRA API resources are enabled.", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg.Client().RESTConfig())
			if err != nil {
				t.Errorf("Failed to create discovery client: %v", err)
				return ctx
			}
			resources, err := discoveryClient.ServerResourcesForGroupVersion("resource.k8s.io/v1")
			if err != nil {
				t.Errorf("the resource.k8s.io/v1 is not enabled: %v", err)
				return ctx
			}
			if resources == nil {
				t.Errorf("no resources found in resource.k8s.io/v1")
				return ctx
			}
			return ctx
		})

	testenv.Test(t, f.Feature())
}
