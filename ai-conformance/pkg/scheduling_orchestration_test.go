package pkg

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func init() {
	utilruntime.Must(kueuev1beta1.AddToScheme(clientgoscheme.Scheme))
}

func TestGangScheduling(t *testing.T) {
	description := "The platform must allow for the installation and successful operation of at least one gang scheduling solution " +
		"that ensures all-or-nothing scheduling for distributed AI workloads (e.g. Kueue, Volcano, etc.) To be conformant, " +
		"the vendor must demonstrate that their platform can successfully run at least one such solution."

	f := features.New("kueue_gang_scheduling_with_two_jobs").
		WithLabel("type", "scheduling_orchestration").
		WithLabel("id", "gang_scheduling").
		WithLabel("level", "MUST").
		WithLabel("scheduler", "kueue").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			SkipIfGroupVersionUnavaliable(t, cfg, "kueue.x-k8s.io/v1beta1")

			// Evaluate the number of avaliable Nvidia GPU(s) in the cluster.
			avaliableGPUs, err := EvaluateAvaliableGPUsExcludingKarpenterNodes(ctx, cfg)
			if err != nil {
				t.Errorf("Failed to evaluate avaliable Nvidia GPU(s) in the cluster: %v", err)
				return ctx
			}
			t.Logf("Current avaliable Nvidia GPU(s) in the cluster: %d", avaliableGPUs)

			if avaliableGPUs < 2 {
				t.Skipf("It needs a cluster with at least 2 Nvidia GPU(s).")
				return ctx
			}

			// We configure the gpu flavor by doubling the total gpu allocatable in our cluster,
			// in order to simulate the deadlock scenario with provisioning when kueue doesn't enable
			// the waitForPodsReady feature which is documented in this link:
			// https://kueue.sigs.k8s.io/docs/tasks/manage/setup_wait_for_pods_ready/
			nominalQuota := avaliableGPUs * 2

			// We create two jobs with the same template and each replica requests 1 Nvidia GPU. Also, pay attention
			// to configure the parallelism and completions to be the same as the jobSize, which is 80% of the total
			// avaliable GPUs per job.
			// In this scenario there is not enough resources to run all pods for both jobs at the same time, risking
			// deadlock.
			jobSize := int32(math.Ceil(float64(avaliableGPUs) * 0.8))

			mutateOptions := []decoder.DecodeOption{
				decoder.MutateNamespace(cfg.Namespace()),
				decoder.MutateOption(func(obj k8s.Object) error {
					switch o := obj.(type) {
					case *kueuev1beta1.ClusterQueue:
						t.Logf("Mutate ClusterQueue %q NominalQuota to %d", o.Name, nominalQuota)
						o.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota = resource.MustParse(strconv.Itoa(nominalQuota))
						return nil
					case *batchv1.Job:
						t.Logf("Mutate Job %s parallelism and completions to %d", o.Name, jobSize)
						o.Spec.Parallelism = &jobSize
						o.Spec.Completions = &jobSize
						o.Spec.Template.Spec.Containers[0].Args[1] = strconv.Itoa(int(jobSize))
						return nil
					}
					return nil
				}),
			}

			// Check if karpenter is installed. If installed, karpenter might provision new nodes to place our jobs, it is not what we want.
			// So we need to mutate the job to not be scheduled on karpenter nodes.
			hasKarpenter, err := IsCrdAvailable(cfg.Client().RESTConfig(), schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodePool"})
			if err != nil {
				t.Errorf("Failed to check if karpenter is installed: %v", err)
				return ctx
			}
			if hasKarpenter {
				mutateOptions = append(mutateOptions, decoder.MutateOption(func(obj k8s.Object) error {
					switch o := obj.(type) {
					case *batchv1.Job:
						t.Logf("Mutate Job %s not able to be scheduled on karpenter nodes", o.Name)
						o.Spec.Template.Spec.Affinity = &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
									NodeSelectorTerms: []corev1.NodeSelectorTerm{
										{
											MatchExpressions: []corev1.NodeSelectorRequirement{
												{
													Key:      "karpenter.sh/nodepool",
													Operator: corev1.NodeSelectorOpDoesNotExist,
												},
											},
										},
									},
								},
							},
						}
					}
					return nil
				}))
			}

			err = decoder.ApplyWithManifestDir(ctx, cfg.Client().Resources(), "testdata", "gang_scheduling/kueue/*.yaml", []resources.CreateOption{}, mutateOptions...)
			if err != nil {
				t.Error(err)
				return ctx
			}
			return ctx
		}).
		AssessWithDescription("multiple jobs can be scheduled and succeed one by one", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			objs, err := decoder.DecodeAllFiles(ctx, fs.FS(os.DirFS("testdata")), "gang_scheduling/kueue/job*.yaml", decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Error(err)
				return ctx
			}
			wg := sync.WaitGroup{}
			for _, obj := range objs {
				_, ok := obj.(*batchv1.Job)
				if !ok {
					continue
				}
				wg.Add(1)
				go func(obj k8s.Object) {
					defer wg.Done()
					err := wait.For(conditions.New(cfg.Client().Resources()).JobCompleted(obj))
					if err != nil {
						t.Errorf("Failed to wait for job %s to complete: %v", obj.GetName(), err)
						return
					}
					t.Logf("The job %s has been completed", obj.GetName())
				}(obj)
			}
			wg.Wait()
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			err := decoder.DeleteWithManifestDir(ctx, cfg.Client().Resources(), "testdata", "gang_scheduling/kueue/*.yaml",
				[]resources.DeleteOption{resources.WithDeletePropagation("Foreground")},
				decoder.MutateNamespace(cfg.Namespace()),
			)
			if err != nil {
				t.Error(err)
				return ctx
			}
			return ctx
		})

	testenv.Test(t, f.Feature())
}

// How we might test it: Prepare a node pool with N nodes, configured with a specific accelerator type,
// with min node pool size of N and max size of at least N+1. Assuming 1 accelerator A per node N,
// Create (A*N)+1 Pods, each requesting one accelerator resource from that pool, verify that at least
// one Pod is unschedulable (Pending), and the cluster autoscaler will increase the node count to N+1,
// causing the Pod to be Running. Delete that Pod, then the cluster autoscaler will remove the idle
// ccelerator node, returning the node count to N.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestClusterAutoscaling(t *testing.T) {
	description := "If the platform provides a cluster autoscaler or an equivalent mechanism, it must be able to scale up/down node groups " +
		"containing specific accelerator types based on pending pods requesting those accelerators."

	type PodsContextKey struct{}
	type NodesContextKey struct{}
	f := features.New("cluster_autoscaling").
		WithLabel("type", "scheduling_orchestration").
		WithLabel("id", "cluster_autoscaling").
		WithLabel("level", "MUST").
		WithLabel("cluster_autoscaler", "karpenter").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			SkipIfGroupVersionUnavaliable(t, cfg, "karpenter.sh/v1")

			r := cfg.Client().Resources()
			mutateOptions := []decoder.DecodeOption{
				decoder.MutateNamespace(cfg.Namespace()),
				// Make sure the pod is only scheduled on karpenter nodes.
				decoder.MutateOption(func(obj k8s.Object) error {
					pod, ok := obj.(*corev1.Pod)
					if !ok {
						return fmt.Errorf("expected a pod, got %T", obj)
					}
					pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "karpenter.sh/nodepool",
											Operator: corev1.NodeSelectorOpExists,
										},
									},
								},
							},
						},
					}
					return nil
				}),
			}
			existingNodes := sets.New[string]()
			pods := []*corev1.Pod{}
			var foundPendingPod bool
			createPodFn := func(ctx context.Context, obj k8s.Object) error {
				t.Logf("Creating pod %s", obj.GetName())
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return fmt.Errorf("expected a pod, got %T", obj)
				}
				err := r.Create(ctx, pod)
				if err != nil {
					return err
				}
				pods = append(pods, pod)

				// watch for the pod and triger action based on the event received.
				w := cfg.Client().Resources().Watch(&corev1.PodList{}, resources.WithFieldSelector(labels.FormatLabels(map[string]string{"metadata.name": pod.Name})))
				w = w.WithUpdateFunc(func(updated interface{}) {
					updatedPod, ok := updated.(*corev1.Pod)
					if !ok {
						return
					}
					if pod.Status.Phase == corev1.PodRunning {
						t.Logf("The pod %s is running, creating another pod later", updatedPod.Name)
						w.Stop()
					} else if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
						t.Logf("The pod %s is failed or succeeded unexpectedly, creating another pod later", updatedPod.Name)
						w.Stop()
					} else if podutils.FailedToSchedule(updatedPod) {
						foundPendingPod = true
						w.Stop()
						nodes := &corev1.NodeList{}
						err := cfg.Client().Resources().List(ctx, nodes)
						if err != nil {
							t.Error(err)
							return
						}
						for _, node := range nodes.Items {
							existingNodes.Insert(node.Name)
						}
						t.Logf("The pod %s failed to schedule on current nodes: %v", updatedPod.Name, existingNodes.UnsortedList())
					}
				})

				err = w.Start(ctx)
				if err != nil {
					t.Error(err)
				}

				return nil
			}
			for !foundPendingPod {
				err := decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "cluster_autoscaling/karpenter/pod-template.yaml", createPodFn, mutateOptions...)
				if err != nil {
					t.Error(err)
					return ctx
				}
			}
			ctx = context.WithValue(ctx, NodesContextKey{}, existingNodes)
			ctx = context.WithValue(ctx, PodsContextKey{}, pods)
			return ctx
		}).
		AssessWithDescription("should provision a new node when a pod failed to schedule", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pods := ctx.Value(PodsContextKey{}).([]*corev1.Pod)
			pendingPod := pods[len(pods)-1]
			err := wait.For(conditions.New(cfg.Client().Resources()).PodRunning(pendingPod), wait.WithTimeout(5*time.Minute))
			if err != nil {
				t.Errorf("Failed to wait for the pod %s to be running: %v", pendingPod.Name, err)
				return ctx
			}

			t.Logf("The pod %s is running on the node %s", pendingPod.Name, pendingPod.Spec.NodeName)
			existingNodes := ctx.Value(NodesContextKey{}).(sets.Set[string])
			if existingNodes.Has(pendingPod.Spec.NodeName) {
				t.Errorf("The pod %s is scheduled on the the existing node, but it should not be", pendingPod.Name)
				return ctx
			}
			return ctx
		}).
		AssessWithDescription("should reclaim the node when the pod is deleted", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pods := ctx.Value(PodsContextKey{}).([]*corev1.Pod)
			toBeDeletedPod := pods[len(pods)-1]
			nodeName := toBeDeletedPod.Spec.NodeName
			err := cfg.Client().Resources().Delete(ctx, toBeDeletedPod)
			if err != nil {
				t.Errorf("Failed to delete the pod %s: %v", toBeDeletedPod.Name, err)
				return ctx
			}
			err = wait.For(conditions.New(cfg.Client().Resources()).ResourceDeleted(toBeDeletedPod))
			if err != nil {
				t.Errorf("Failed to wait for the pod %s to be deleted: %v", toBeDeletedPod.Name, err)
				return ctx
			}

			err = wait.For(conditions.New(cfg.Client().Resources()).ResourceDeleted(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}), wait.WithTimeout(10*time.Minute))
			if err != nil {
				t.Errorf("Failed to wait for the node %s to be reclaimed: %v", nodeName, err)
				return ctx
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pods := ctx.Value(PodsContextKey{}).([]*corev1.Pod)
			for _, pod := range pods {
				err := cfg.Client().Resources().Delete(ctx, pod)
				if err != nil && !apierrors.IsNotFound(err) {
					t.Error(err)
					continue
				}
			}
			return ctx
		})

	testenv.Test(t, f.Feature())
}

// How we might test it: A custom metrics pipeline is configured to expose accelerator-related
// custom metrics to the HPA. Create a Deployment with each Pod requests an accelerator and
// exposes a custom metric. Create an HorizontalPodAutoscaler targeting the Deployment.
// Introduce load to the sample application, causing the average custom metric value to significantly
// exceed the target, triggering a scale up. Then remove the load to trigger a scale down.
//
// See https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0
func TestPodAutoscaling(t *testing.T) {
	description := "If the platform supports the HorizontalPodAutoscaler, it must function correctly for pods utilizing accelerators. " +
		"This includes the ability to scale these Pods based on custom metrics relevant to AI/ML workloads."

	type AIWorkloadContextKey struct{}
	type HPAContextKey struct{}
	loadTestJobName := "loadtest"
	f := features.New("pod_autoscaling").
		WithLabel("type", "scheduling_orchestration").
		WithLabel("id", "pod_autoscaling").
		WithLabel("level", "MUST").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			SkipIfGroupVersionUnavaliable(t, cfg, "custom.metrics.k8s.io/v1beta1")

			// I'm unsure whether we need to support other workload types like lws,
			// but for now we only have deployment for AI workload.
			var workload k8s.Object
			hpa := &autoscalingv2.HorizontalPodAutoscaler{}
			workloadFn := func(ctx context.Context, obj k8s.Object) error {
				switch o := obj.(type) {
				case *batchv1.Job:
					// If the job is used for load testing, we should not create it before workload is ready.
					if o.GetName() == loadTestJobName {
						return nil
					}
				case *appsv1.Deployment:
					workload = o
				case *autoscalingv2.HorizontalPodAutoscaler:
					hpa = o
				}
				t.Logf("Creating %s %s", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName())

				err := cfg.Client().Resources().Create(ctx, obj)
				if err != nil {
					return err
				}
				return nil
			}
			err := decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "pod_autoscaling/*.yaml", workloadFn, decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Error(err)
				return ctx
			}

			// Wait for the workload to has exactly the same number of replicas as the ready replicas.
			err = wait.For(conditions.New(cfg.Client().Resources()).ResourceMatch(workload, func(obj k8s.Object) bool {
				switch o := obj.(type) {
				case *appsv1.Deployment:
					return ptr.Deref(o.Spec.Replicas, 0) == o.Status.ReadyReplicas
				default:
					return true
				}
			}))
			if err != nil {
				t.Errorf("Failed to wait for the workload %s to be ready: %v", workload.GetName(), err)
				return ctx
			}

			loadtestFn := func(ctx context.Context, obj k8s.Object) error {
				job, ok := obj.(*batchv1.Job)
				if !ok {
					return nil
				}
				t.Logf("Creating Job %s", job.GetName())
				return cfg.Client().Resources().Create(ctx, job)
			}
			err = decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "pod_autoscaling/*.yaml", loadtestFn, decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Error(err)
				return ctx
			}

			ctx = context.WithValue(ctx, HPAContextKey{}, hpa)
			ctx = context.WithValue(ctx, AIWorkloadContextKey{}, workload)
			return ctx
		}).
		AssessWithDescription("should scale up and down the workload based on the custom metrics", description, func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r := cfg.Client().Resources()
			workload := ctx.Value(AIWorkloadContextKey{}).(k8s.Object)
			oldWorkload := workload.DeepCopyObject()
			hpa := ctx.Value(HPAContextKey{}).(*autoscalingv2.HorizontalPodAutoscaler)
			minReplicas := ptr.Deref(hpa.Spec.MinReplicas, 1)

			scaledUpCtx, scaledUpCancel := context.WithCancel(ctx)
			go func(ctx context.Context) {
				defer scaledUpCancel()
				err := wait.For(conditions.New(r).JobCompleted(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace(), Name: loadTestJobName}}), wait.WithContext(ctx))
				if err != nil {
					t.Errorf("Failed to wait for the load test job to complete: %v", err)
					return
				}
				t.Logf("The load test job has been completed")
			}(ctx)

			// Wait for the workload to be scaled up.
			var scaledUpFn = func(_ k8s.Object) bool {
				switch o := workload.(type) {
				case *appsv1.Deployment:
					updated := workload.(*appsv1.Deployment)
					old := oldWorkload.(*appsv1.Deployment)
					initialReplicas := ptr.Deref(old.Spec.Replicas, 1)
					updatedReplicas := ptr.Deref(updated.Spec.Replicas, 1)
					if initialReplicas < updatedReplicas {
						t.Logf("The Deployment %s is being scaled up from %d to %d", o.GetName(), initialReplicas, updatedReplicas)
						return true
					}
					return false
				default:
					return true
				}
			}
			err := wait.For(conditions.New(r).ResourceMatch(workload, scaledUpFn), wait.WithContext(scaledUpCtx))
			if err != nil {
				t.Errorf("Failed to wait for the workload %s to be scaled up: %v", workload.GetName(), err)
				return ctx
			}

			// Wait for the workload to be scaled down to min replicas.
			err = wait.For(conditions.New(r).ResourceScaled(workload, func(obj k8s.Object) int32 {
				switch o := obj.(type) {
				case *appsv1.Deployment:
					return *o.Spec.Replicas
				default:
					return 0
				}
			}, minReplicas))
			if err != nil {
				t.Errorf("Failed to wait for the workload %s to be scaled down to min replicas %d: %v", workload.GetName(), minReplicas, err)
				return ctx
			}
			t.Logf("The workload %s has been scaled down to min replicas %d", workload.GetName(), minReplicas)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			err := decoder.DecodeEachFile(ctx, fs.FS(os.DirFS("testdata")), "pod_autoscaling/*.yaml",
				decoder.DeleteIgnoreNotFound(cfg.Client().Resources(), resources.WithDeletePropagation("Foreground")),
				decoder.MutateNamespace(cfg.Namespace()))
			if err != nil {
				t.Error(err)
			}
			return ctx
		})

	testenv.Test(t, f.Feature())
}
