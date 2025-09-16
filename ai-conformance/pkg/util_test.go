package pkg

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	resourcehelper "k8s.io/component-helpers/resource"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

var gvResourcesCache map[string]*metav1.APIResourceList

// IsCrdAvailable checks if a given CRD is present in the cluster by verifying the
// existence of its API.
func IsCrdAvailable(config *rest.Config, gvk schema.GroupVersionKind) (bool, error) {
	gvResources, err := GetAvailableResourcesForApi(config, gvk.GroupVersion().String())
	if err != nil {
		return false, err
	}

	found := false
	if gvResources != nil {
		for _, crd := range gvResources.APIResources {
			if crd.Kind == gvk.Kind {
				found = true
				break
			}
		}
	}

	return found, nil
}

// GetAvailableResourcesForApi returns the list of discovered resources that belong
// to the API specified in groupVersion. The first query to a specifig groupVersion will
// query the cluster API server to discover the available resources and the discovered
// resources will be cached and returned to subsequent invocations to prevent additional
// queries to the API server.
func GetAvailableResourcesForApi(config *rest.Config, groupVersion string) (*metav1.APIResourceList, error) {
	var gvResources *metav1.APIResourceList
	var ok bool

	if gvResources, ok = gvResourcesCache[groupVersion]; !ok {
		discoveryClient, newClientErr := discovery.NewDiscoveryClientForConfig(config)
		if newClientErr != nil {
			return nil, newClientErr
		}

		var getGvResourcesErr error
		gvResources, getGvResourcesErr = discoveryClient.ServerResourcesForGroupVersion(groupVersion)
		if getGvResourcesErr != nil && !apierrors.IsNotFound(getGvResourcesErr) {
			return nil, getGvResourcesErr
		}

		SetAvailableResourcesForApi(groupVersion, gvResources)
	}

	return gvResources, nil
}

// SetAvailableResourcesForApi stores the value fo resources argument in the global cache
// of discovered API resources. This function should never be called directly. It is exported
// for usage in tests.
func SetAvailableResourcesForApi(groupVersion string, resources *metav1.APIResourceList) {
	if gvResourcesCache == nil {
		gvResourcesCache = make(map[string]*metav1.APIResourceList)
	}

	gvResourcesCache[groupVersion] = resources
}

func SkipIfGroupVersionUnavaliable(t *testing.T, cfg *envconf.Config, groupVersion string) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg.Client().RESTConfig())
	if err != nil {
		t.Errorf("Failed to create discovery client: %v", err)
		return
	}
	_, err = discoveryClient.ServerResourcesForGroupVersion(groupVersion)
	if err == nil {
		return
	}
	if apierrors.IsNotFound(err) {
		t.Skipf("%s is not found", groupVersion)
		return
	}
	t.Errorf("failed to get resources in %s: %v", groupVersion, err)
}

func SkipIfResourceUnavailable(t *testing.T, cfg *envconf.Config, gvk schema.GroupVersionKind) {
	found, err := IsCrdAvailable(cfg.Client().RESTConfig(), gvk)
	if err != nil {
		t.Fatalf("Failed to check %s: %v", gvk, err)
	}
	if !found {
		t.Skipf("%s is unavaliable", gvk)
	}
}

// EvaluateAvaliableGPUsExcludingKarpenter evaluates the number of avaliable GPUs in the cluster excluding karpenter nodes.
func EvaluateAvaliableGPUsExcludingKarpenterNodes(ctx context.Context, cfg *envconf.Config) (int, error) {
	client := cfg.Client().Resources()

	nodes := &corev1.NodeList{}
	if err := client.List(ctx, nodes); err != nil {
		return 0, err
	}

	totalGPUs := 0
	ingoreNodes := sets.New[string]()
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable || len(node.Spec.Taints) > 0 || metav1.HasLabel(node.ObjectMeta, "karpenter.sh/nodepool") {
			ingoreNodes.Insert(node.Name)
			continue
		}
		for resource, count := range node.Status.Allocatable {
			if resource == corev1.ResourceName("nvidia.com/gpu") {
				totalGPUs += int(count.Value())
			}
		}
	}

	pods := &corev1.PodList{}
	if err := client.List(ctx, pods); err != nil {
		return 0, err
	}
	usedGPUs := 0
	for _, pod := range pods.Items {
		if ingoreNodes.Has(pod.Spec.NodeName) {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for resource, count := range resourcehelper.PodLimits(&pod, resourcehelper.PodResourcesOptions{}) {
			if resource == corev1.ResourceName("nvidia.com/gpu") {
				usedGPUs += int(count.Value())
			}
		}
	}
	return totalGPUs - usedGPUs, nil
}

// RenderOptions holds options for the helm template command.
type RenderOptions struct {
	Name        string
	Namespace   string
	Chart       string
	Version     string
	Repo        string
	ValuesFile  string
	IncludeCrds bool
	SetPairs    []string // list of key=val pairs (can include commas in each)
	ExtraArgs   []string // any extra args to pass through to helm
}

// BuildHelmTemplateCmd builds the exec.Command for `helm template`.
func BuildHelmTemplateCmd(opts RenderOptions) (*exec.Cmd, error) {
	if opts.Chart == "" {
		return nil, fmt.Errorf("release name is required")
	}
	// base args
	args := []string{"template", opts.Name, opts.Chart}

	if opts.Namespace != "" {
		args = append(args, "--namespace", opts.Namespace)
	}

	// version and repo
	if opts.Version != "" {
		args = append(args, "--version", opts.Version)
	}
	if opts.Repo != "" {
		args = append(args, "--repo", opts.Repo)
	}

	// values file
	if opts.ValuesFile != "" {
		args = append(args, "--values", opts.ValuesFile)
	}

	if opts.IncludeCrds {
		args = append(args, "--include-crds")
	}

	// --set: accept multiple pairs; each element might itself contain commas
	// Helm supports multiple --set occurrences as well
	for _, p := range opts.SetPairs {
		if strings.TrimSpace(p) == "" {
			continue
		}
		args = append(args, "--set", p)
	}

	// extra args passthrough
	if len(opts.ExtraArgs) > 0 {
		args = append(args, opts.ExtraArgs...)
	}

	cmd := exec.Command("helm", args...)
	return cmd, nil
}

// RenderHelmTemplate runs helm template and returns rendered manifest (stdout) and any combined stderr.
func RenderHelmTemplate(opts RenderOptions) (stdout string, stderr string, err error) {
	cmd, err := BuildHelmTemplateCmd(opts)
	if err != nil {
		return "", "", err
	}
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	// Run the command
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}
