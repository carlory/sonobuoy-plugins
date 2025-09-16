package pkg

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubectl/pkg/util/podutils"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/third_party/helm"
)

// How we might test it: Deploy a representative AI operator, verify all Pods of the operator
// and its webhook are Running and its CRDs are registered with the API server. Verify that
// invalid attempts (e.g. invalid spec) should be rejected by its admission webhook. Verify
// that a valid instance of the custom resource can be reconciled.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestRobustController(t *testing.T) {
	description := "The platform must prove that at least one complex AI operator with a CRD (e.g., Ray, Kubeflow) can be installed and functions reliably. " +
		"This includes verifying that the operator's pods run correctly, its webhooks are operational, and its custom resources can be reconciled."

	type RenderOptionsContextKey struct{}
	type ResourcesContextKey struct{}
	type OperatorNamespaceContextKey struct{}
	type ValidatingWebhookConfigurationsContextKey struct{}
	type CRDsContextKey struct{}
	f := features.New("robust_controller").
		WithLabel("type", "operator").
		WithLabel("id", "robust_controller").
		WithLabel("level", "MUST").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// FIXME: remove this when all test case are added.
			os.Setenv("https_proxy", "http://127.0.0.1:7897")
			os.Setenv("http_proxy", "http://127.0.0.1:7897")
			os.Setenv("all_proxy", "socks5://127.0.0.1:7897")
			defer os.Unsetenv("https_proxy")
			defer os.Unsetenv("http_proxy")
			defer os.Unsetenv("all_proxy")

			renderOpts := RenderOptions{
				Name:        "kuberay-operator",
				Namespace:   "kuberay-operator",
				Chart:       "kuberay-operator",
				Repo:        "https://ray-project.github.io/kuberay-helm/",
				IncludeCrds: true,
			}
			manifests, stderr, err := RenderHelmTemplate(renderOpts)
			if err != nil {
				t.Errorf("error when rendering helm template: %v, stderr: %s", err, stderr)
				return ctx
			}
			if stderr != "" {
				t.Errorf("error when rendering helm template: %s", stderr)
				return ctx
			}
			// convert the manifests to objects
			objects := []k8s.Object{}
			err = decoder.DecodeEach(ctx, strings.NewReader(manifests), func(ctx context.Context, obj k8s.Object) error {
				t.Logf("Found %s with name %s from the chart template %s", obj.GetObjectKind().GroupVersionKind(), obj.GetName(), renderOpts.Chart)
				objects = append(objects, obj)
				return nil
			})
			if err != nil {
				t.Errorf("error when decoding manifests: %v", err)
				return ctx
			}

			// Install the helm chart
			manager := helm.New(cfg.KubeconfigFile())
			err = manager.RunInstall(
				helm.WithName(renderOpts.Name),
				helm.WithChart(renderOpts.Chart),
				helm.WithArgs("--repo", renderOpts.Repo),
				helm.WithNamespace(renderOpts.Namespace),
				helm.WithArgs("--create-namespace"),
				helm.WithArgs("--debug"),
				helm.WithWait(),
				helm.WithTimeout("15m"),
			)
			if err != nil {
				t.Errorf("error when installing helm chart: %v", err)
				return ctx
			}
			t.Logf("The chart %s is installed", renderOpts.Chart)
			ctx = context.WithValue(ctx, RenderOptionsContextKey{}, renderOpts)
			ctx = context.WithValue(ctx, OperatorNamespaceContextKey{}, renderOpts.Namespace)
			ctx = context.WithValue(ctx, ResourcesContextKey{}, objects)
			return ctx
		}).
		AssessWithDescription("verify all Pods of the operator and its webhook are Running and its CRDs are registered with the API server", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			operatorNamespace := ctx.Value(OperatorNamespaceContextKey{}).(string)
			objects := ctx.Value(ResourcesContextKey{}).([]k8s.Object)

			pods := &corev1.PodList{}
			err := client.Resources(operatorNamespace).List(ctx, pods)
			if err != nil {
				t.Errorf("error when getting Pods in the namespace: %s: %v", operatorNamespace, err)
				return ctx
			}
			err = wait.For(conditions.New(cfg.Client().Resources()).ResourcesMatch(pods, func(obj k8s.Object) bool {
				return podutils.IsPodReady(obj.(*corev1.Pod))
			}))
			if err != nil {
				t.Errorf("error when checking all Pods ready in the namespace %s: %v", operatorNamespace, err)
				return ctx
			}
			t.Logf("All Pods in the namespace %s are ready", operatorNamespace)

			var checkWebhookFn = func(ctx context.Context, namespace, name string) error {
				svc := &corev1.Service{}
				err := client.Resources().Get(ctx, name, namespace, svc)
				if err != nil {
					return err
				}
				pods := &corev1.PodList{}
				err = client.Resources().List(ctx, pods, resources.WithLabelSelector(labels.FormatLabels(svc.Spec.Selector)))
				if err != nil {
					return err
				}
				return wait.For(conditions.New(cfg.Client().Resources()).ResourcesMatch(pods, func(obj k8s.Object) bool {
					return podutils.IsPodReady(obj.(*corev1.Pod))
				}))
			}

			var checkCrdFn = func(_ context.Context, obj k8s.Object) error {
				err := wait.For(conditions.New(cfg.Client().Resources()).ResourceMatch(obj, func(obj k8s.Object) bool {
					conditions := obj.(*apiextensionsv1.CustomResourceDefinition).Status.Conditions
					hasEstablishedCondition := false
					hasNamesAcceptedCondition := false
					for _, cond := range conditions {
						if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
							hasEstablishedCondition = true
						}
						if cond.Type == apiextensionsv1.NamesAccepted && cond.Status == apiextensionsv1.ConditionTrue {
							hasNamesAcceptedCondition = true
						}
					}
					return hasEstablishedCondition && hasNamesAcceptedCondition
				}))
				return err
			}

			validatingWebhookConfigurations := []*admissionregistrationv1.ValidatingWebhookConfiguration{}
			crds := []*apiextensionsv1.CustomResourceDefinition{}
			for _, obj := range objects {
				switch o := obj.(type) {
				case *appsv1.Deployment:
					err := wait.For(conditions.New(cfg.Client().Resources()).DeploymentAvailable(o.GetName(), o.GetNamespace()))
					if err != nil {
						t.Errorf("error when checking Deployment %s: %v", o.GetName(), err)
						return ctx
					}
					t.Logf("Deployment %s is ready", o.GetName())
				case *appsv1.DaemonSet:
					err := wait.For(conditions.New(cfg.Client().Resources()).DaemonSetReady(o))
					if err != nil {
						t.Errorf("error when checking DaemonSet %s: %v", o.GetName(), err)
						return ctx
					}
					t.Logf("DaemonSet %s is ready", o.GetName())
				case *appsv1.StatefulSet:
					err := wait.For(conditions.New(cfg.Client().Resources()).ResourceMatch(o, func(obj k8s.Object) bool {
						return obj.(*appsv1.StatefulSet).Status.ReadyReplicas == *o.Spec.Replicas
					}))
					if err != nil {
						t.Errorf("error when checking StatefulSet %s: %v", o.GetName(), err)
						return ctx
					}
					t.Logf("StatefulSet %s is ready", o.GetName())
				case *admissionregistrationv1.ValidatingWebhookConfiguration:
					validatingWebhookConfigurations = append(validatingWebhookConfigurations, o)
					for _, webhook := range o.Webhooks {
						if webhook.ClientConfig.Service != nil {
							namespace := webhook.ClientConfig.Service.Namespace
							name := webhook.ClientConfig.Service.Name
							if err := checkWebhookFn(ctx, namespace, name); err != nil {
								t.Errorf("error when checking service %s for ValidatingWebhookConfiguration %s: %v", name, o.GetName(), err)
								return ctx
							}
						}
					}
					t.Logf("ValidatingWebhookConfiguration %s is ready", o.GetName())
				case *admissionregistrationv1.MutatingWebhookConfiguration:
					for _, webhook := range o.Webhooks {
						if webhook.ClientConfig.Service != nil {
							namespace := webhook.ClientConfig.Service.Namespace
							name := webhook.ClientConfig.Service.Name
							if err := checkWebhookFn(ctx, namespace, name); err != nil {
								t.Errorf("error when checking service %s for ValidatingWebhookConfiguration %s: %v", name, o.GetName(), err)
								return ctx
							}
						}
					}
					t.Logf("MutatingWebhookConfiguration %s is ready", o.GetName())
				case *apiextensionsv1.CustomResourceDefinition:
					crds = append(crds, o)
					if err := checkCrdFn(ctx, o); err != nil {
						t.Errorf("error when checking CRD %s: %v", o.GetName(), err)
						return ctx
					}
					t.Logf("CustomResourceDefinition %s is ready", o.GetName())
				}
			}

			ctx = context.WithValue(ctx, ValidatingWebhookConfigurationsContextKey{}, validatingWebhookConfigurations)
			ctx = context.WithValue(ctx, CRDsContextKey{}, crds)
			return ctx
		}).
		AssessWithDescription("verify that invalid attempts (e.g. invalid spec) should be rejected by its admission webhook", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			validatingWebhookConfigurations := ctx.Value(ValidatingWebhookConfigurationsContextKey{}).([]*admissionregistrationv1.ValidatingWebhookConfiguration)
			if len(validatingWebhookConfigurations) == 0 {
				t.Skipf("no validating webhook configuration found")
				return ctx
			}

			var checkValidatingWebhookConfigurations = func(_ k8s.Object) bool {
				return true
			}

			var handlerFn = func(ctx context.Context, obj k8s.Object) error {
				if !checkValidatingWebhookConfigurations(obj) {
					t.Errorf("%s %s is not handled by all validating webhook configurations", obj.GetObjectKind().GroupVersionKind(), obj.GetName())
					return nil
				}
				t.Logf("Try creating an invalid %s", obj.GetObjectKind().GroupVersionKind())
				err := cfg.Client().Resources().Create(ctx, obj)
				if err == nil {
					t.Errorf("%s is created successfully, but it should be rejected by the admission webhook", obj.GetName())
					return nil
				}
				if !apierrors.IsForbidden(err) {
					t.Errorf("expected a forbidden error, but got %v", err)
					return nil
				}
				t.Logf("%s is rejected by the admission webhook", obj.GetName())
				return nil
			}

			err := decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "robust_controller/invalid.yaml", handlerFn, decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Errorf("error when decoding invalid custom resource: %v", err)
				return ctx
			}
			return ctx
		}).
		AssessWithDescription("verify that a valid instance of the custom resource can be reconciled", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			crds := ctx.Value(CRDsContextKey{}).([]*apiextensionsv1.CustomResourceDefinition)
			if len(crds) == 0 {
				t.Errorf("no custom resource definition found for the operator")
				return ctx
			}
			// TODO: add something to test the reconciliation
			objs, err := decoder.DecodeAllFiles(ctx, fs.FS(os.DirFS("testdata")), "robust_controller/valid.yaml", decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Errorf("error when decoding files %v", err)
				return ctx
			}

			allCRs := []k8s.Object{}
			reconcilableCRs := map[schema.GroupVersionResource][]k8s.Object{}
			for _, obj := range objs {
				objGvk := obj.GetObjectKind().GroupVersionKind()
				for _, crd := range crds {
					// check api group and kind
					if objGvk.Group != crd.Spec.Group || objGvk.Kind != crd.Spec.Names.Kind {
						continue
					}
					// check api version
					found := false
					for _, version := range crd.Spec.Versions {
						if objGvk.Version == version.Name {
							found = true

							// check if the custom resource has status subresource
							if version.Subresources != nil && version.Subresources.Status != nil {
								gvr := schema.GroupVersionResource{
									Group:    crd.Spec.Group,
									Version:  version.Name,
									Resource: crd.Spec.Names.Plural,
								}
								reconcilableCRs[gvr] = append(reconcilableCRs[gvr], obj)
							}
							break
						}
					}
					if !found {
						continue
					}
					allCRs = append(allCRs, obj)

				}
			}

			if len(allCRs) == 0 {
				t.Errorf("no custom resource found which is introduced by the operator: %v", crds)
				return ctx
			}

			if len(reconcilableCRs) == 0 {
				t.Errorf("no reconcilable custom resource found in all custom resources: %v", allCRs)
				return ctx
			}

			err = decoder.ApplyWithManifestDir(ctx, client.Resources(), "testdata", "robust_controller/valid.yaml", []resources.CreateOption{}, decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Errorf("error when applying resources from valid.yaml: %v", err)
				return ctx
			}

			dynClient, err := dynamic.NewForConfig(cfg.Client().RESTConfig())
			if err != nil {
				panic(err)
			}

			// Check if the custom resource is reconciled
			wg := sync.WaitGroup{}
			for gvr, objs := range reconcilableCRs {
				for _, obj := range objs {
					wg.Add(1)
					go func(obj k8s.Object) {
						defer wg.Done()

						namespace := obj.GetNamespace()
						name := obj.GetName()

						opts := metav1.ListOptions{
							FieldSelector: fmt.Sprintf("metadata.name=%s", name),
						}
						timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
						defer cancel()
						watcher, err := dynClient.Resource(gvr).Namespace(namespace).Watch(timeoutCtx, opts)
						if err != nil {
							t.Errorf("error when watching %s %s: %v", gvr.Group, gvr.Resource, err)
							return
						}
						for event := range watcher.ResultChan() {
							switch event.Type {
							case watch.Modified:
								t.Logf("MODIFIED: %v\n", event.Object)
								watcher.Stop()
								return
							case watch.Error:
								t.Errorf("ERROR: %v\n", event.Object)
								watcher.Stop()
								return
							}
						}

					}(obj)
				}
			}
			wg.Wait()
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			renderOpts := ctx.Value(RenderOptionsContextKey{}).(RenderOptions)

			err := decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "robust_controller/*.yaml",
				decoder.DeleteIgnoreNotFound(cfg.Client().Resources(), resources.WithDeletePropagation("Foreground")),
				decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Error(err)
			}
			// FIXME: we should wait for the resource deleted
			time.Sleep(30 * time.Second)

			// Uninstall the helm chart
			manager := helm.New(cfg.KubeconfigFile())
			err = manager.RunUninstall(
				helm.WithNamespace(renderOpts.Namespace),
				helm.WithName(renderOpts.Name),
				helm.WithArgs("--debug"),
				helm.WithWait(),
			)
			if err != nil {
				t.Error(err)
			}
			return ctx
		})

	testenv.Test(t, f.Feature())
}
