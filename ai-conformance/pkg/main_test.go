package pkg

import (
	"context"
	"os"
	"testing"
	"time"

	plugin_helper "github.com/vmware-tanzu/sonobuoy-plugins/plugin-helper"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"
)

var testenv env.Environment

func TestMain(m *testing.M) {
	cfg, _ := envconf.NewFromFlags()
	testenv = env.NewWithConfig(cfg)
	kindClusterName := "kind-ai-conformance"
	namespace := "cncf-ai-conformance"

	testenv.Setup(
		// Create kind cluster with custom config
		envfuncs.CreateClusterWithOpts(
			kind.NewProvider(),
			kindClusterName,
			// Required by the dra_support test
			kind.WithImage("kindest/node:v1.34.0"),
		),
		// Create the namespace for the tests to run in.
		envfuncs.CreateNamespace(namespace),
		// Install gateway crds which are required by the ai_inference test
		envfuncs.SetupCRDs("testdata/crds/gateway-api", "*"),
		// Install kueue via helm
		// func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		// 	os.Setenv("https_proxy", "http://127.0.0.1:7897")
		// 	os.Setenv("http_proxy", "http://127.0.0.1:7897")
		// 	os.Setenv("all_proxy", "socks5://127.0.0.1:7897")
		// 	defer os.Unsetenv("https_proxy")
		// 	defer os.Unsetenv("http_proxy")
		// 	defer os.Unsetenv("all_proxy")

		// 	manager := helm.New(cfg.KubeconfigFile())

		// 	err := manager.RunInstall(
		// 		helm.WithName("kueue"),
		// 		helm.WithChart("oci://registry.k8s.io/kueue/charts/kueue"),
		// 		helm.WithVersion("0.13.4"),
		// 		helm.WithNamespace("kueue-system"),
		// 		helm.WithArgs("--create-namespace", "-f", "testdata/helm/kueue/values.yaml"),
		// 		helm.WithArgs("--debug"),
		// 		helm.WithWait(),
		// 		helm.WithTimeout("15m"),
		// 	)
		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	return ctx, nil
		// },
		// // Install fake gpu operator via helm which is used for a KinD cluster to
		// // test the gang scheduling orchestration.
		// func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		// 	// Selected nodes to simulate themselvesas GPU nodes.
		// 	nodes := &corev1.NodeList{}
		// 	if err := cfg.Client().Resources().List(ctx, nodes); err != nil {
		// 		return nil, err
		// 	}
		// 	mergePatch, err := json.Marshal(map[string]interface{}{
		// 		"metadata": map[string]interface{}{
		// 			"labels": map[string]interface{}{
		// 				"run.ai/simulated-gpu-node-pool": "default",
		// 			},
		// 		},
		// 	})
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	for _, node := range nodes.Items {
		// 		err := cfg.Client().Resources().Patch(ctx, &node, k8s.Patch{PatchType: types.StrategicMergePatchType, Data: mergePatch})
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 	}

		// 	// Install fake gpu operator via helm.
		// 	manager := helm.New(cfg.KubeconfigFile())
		// 	err = manager.RunInstall(
		// 		helm.WithName("gpu-operator"),
		// 		helm.WithChart("oci://ghcr.io/run-ai/fake-gpu-operator/fake-gpu-operator"),
		// 		helm.WithVersion("0.0.63"),
		// 		helm.WithNamespace("gpu-operator"),
		// 		helm.WithArgs("--create-namespace", "-f", "testdata/helm/fake-gpu-operator/values.yaml"),
		// 		helm.WithArgs("--debug"),
		// 		helm.WithWait(),
		// 		helm.WithTimeout("10m"),
		// 	)
		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	// Wait for kubelet to report Nvidia GPU(s) resources to the API server.
		// 	wait.For(conditions.New(cfg.Client().Resources()).ResourcesMatch(nodes, func(object k8s.Object) bool {
		// 		node := object.(*corev1.Node)
		// 		if _, ok := node.Labels["run.ai/simulated-gpu-node-pool"]; !ok {
		// 			return true
		// 		}
		// 		if gpu, ok := node.Status.Allocatable["nvidia.com/gpu"]; ok && !gpu.IsZero() {
		// 			return true
		// 		}
		// 		return false
		// 	}))
		// 	return ctx, nil
		// },
		// // Install prometheus and prometheus-adapter via helm
		// func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		// 	manager := helm.New(cfg.KubeconfigFile())

		// 	err := manager.RunInstall(
		// 		helm.WithName("kube-prometheus-stack"),
		// 		helm.WithChart("oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack"),
		// 		helm.WithNamespace("monitoring"),
		// 		helm.WithArgs("--create-namespace", "-f", "testdata/helm/kube-prometheus-stack/values.yaml"),
		// 		helm.WithArgs("--debug"),
		// 		helm.WithWait(),
		// 		helm.WithTimeout("15m"),
		// 	)

		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	err = manager.RunInstall(
		// 		helm.WithName("prometheus-adapter"),
		// 		helm.WithChart("oci://ghcr.io/prometheus-community/charts/prometheus-adapter"),
		// 		helm.WithNamespace("monitoring"),
		// 		helm.WithArgs("--create-namespace", "-f", "testdata/helm/prometheus-adapter/values.yaml", "--set", "prometheus.url=http://kube-prometheus-stack-prometheus.monitoring.svc"),
		// 		helm.WithArgs("--debug"),
		// 		helm.WithWait(),
		// 		helm.WithTimeout("15m"),
		// 	)
		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	return ctx, nil
		// },
		// Sleep for 2 seconds to aviod the race between kube-apiserver and our tests.
		func(ctx context.Context, _ *envconf.Config) (context.Context, error) {
			time.Sleep(2 * time.Second)
			return ctx, nil
		},
	)
	// testenv.Finish(
	// 	// Uninstall fake gpu operator via helm
	// 	func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	// 		manager := helm.New(cfg.KubeconfigFile())
	// 		err := manager.RunUninstall(
	// 			helm.WithNamespace("gpu-operator"),
	// 			helm.WithName("gpu-operator"),
	// 			helm.WithArgs("--debug"),
	// 			helm.WithWait(),
	// 		)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		err = cfg.Client().Resources().Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "gpu-operator"}})
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		return ctx, nil
	// 	},
	// 	// Uninstall kueue via helm
	// 	func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	// 		manager := helm.New(cfg.KubeconfigFile())
	// 		err := manager.RunUninstall(
	// 			helm.WithNamespace("kueue-system"),
	// 			helm.WithName("kueue"),
	// 			helm.WithArgs("--debug"),
	// 			helm.WithWait(),
	// 		)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		err = cfg.Client().Resources().Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kueue-system"}})
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		return ctx, nil
	// 	},
	// 	// Uninstall prometheus and prometheus-adapter via helm
	// 	func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	// 		manager := helm.New(cfg.KubeconfigFile())

	// 		err := manager.RunUninstall(
	// 			helm.WithNamespace("monitoring"),
	// 			helm.WithName("kube-prometheus-stack"),
	// 			helm.WithArgs("--debug"),
	// 			helm.WithWait(),
	// 		)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		err = manager.RunUninstall(
	// 			helm.WithNamespace("monitoring"),
	// 			helm.WithName("prometheus-adapter"),
	// 			helm.WithArgs("--debug"),
	// 			helm.WithWait(),
	// 		)
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		err = cfg.Client().Resources().Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "monitoring"}})
	// 		if err != nil && !apierrors.IsNotFound(err) {
	// 			return nil, err
	// 		}
	// 		return ctx, nil
	// 	},
	// 	// Delete the namespace for the tests to run in.
	// 	envfuncs.DeleteNamespace(namespace),
	// 	// Delete the gateway crds
	// 	envfuncs.TeardownCRDs("testdata/crds/gateway", "*"),
	// 	// Delete the kind cluster
	// 	envfuncs.DestroyCluster(kindClusterName),
	// )

	updateReporter := plugin_helper.NewProgressReporter(0)
	testenv.BeforeEachTest(func(ctx context.Context, cfg *envconf.Config, t *testing.T) (context.Context, error) {
		updateReporter.StartTest(t.Name())
		return ctx, nil
	})
	testenv.AfterEachTest(func(ctx context.Context, cfg *envconf.Config, t *testing.T) (context.Context, error) {
		updateReporter.StopTest(t.Name(), t.Failed(), t.Skipped(), nil)
		return ctx, nil
	})

	/*
		testenv.BeforeEachFeature(func(ctx context.Context, config *envconf.Config, info features.Feature) (context.Context, error) {
			// Note that you can also add logic here for before a feature is tested. There may be
			// more than one feature in a test.
			return ctx, nil
		})
		testenv.AfterEachFeature(func(ctx context.Context, config *envconf.Config, info features.Feature) (context.Context, error) {
			// Note that you can also add logic here for after a feature is tested. There may be
			// more than one feature in a test.
			return ctx, nil
		})
	*/

	os.Exit(testenv.Run(m))
}
