package controller

import (
	"context"
	"reflect"
	"sync"

	observabilityv1alpha1 "github.com/influxdata/influx-log-viewer/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Runner interface{ Run(context.Context) error }
type RunnerFactory func(*observabilityv1alpha1.LogPipeline) Runner
type activeRunner struct {
	generation int64
	cancel     context.CancelFunc
}

type LogPipelineReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	factory RunnerFactory
	mu      sync.Mutex
	active  map[types.NamespacedName]activeRunner
}

func NewLogPipelineReconciler(cl client.Client, scheme *runtime.Scheme, factory RunnerFactory) *LogPipelineReconciler {
	return &LogPipelineReconciler{Client: cl, Scheme: scheme, factory: factory, active: make(map[types.NamespacedName]activeRunner)}
}

// +kubebuilder:rbac:groups=observability.influxdata.com,resources=logpipelines,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=observability.influxdata.com,resources=logpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
func (r *LogPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pipeline observabilityv1alpha1.LogPipeline
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		if apierrors.IsNotFound(err) {
			r.stop(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	before := pipeline.Spec
	pipeline.Default()
	if !reflect.DeepEqual(before, pipeline.Spec) {
		if err := r.Update(ctx, &pipeline); err != nil {
			return ctrl.Result{}, err
		}
	}
	r.mu.Lock()
	current, ok := r.active[req.NamespacedName]
	if ok && current.generation == pipeline.Generation {
		r.mu.Unlock()
		return ctrl.Result{}, nil
	}
	if ok {
		current.cancel()
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.active[req.NamespacedName] = activeRunner{generation: pipeline.Generation, cancel: cancel}
	runner := r.factory(pipeline.DeepCopy())
	r.mu.Unlock()
	go func() { _ = runner.Run(runCtx) }()
	now := metav1.Now()
	pipeline.Status = observabilityv1alpha1.LogPipelineStatus{Phase: "Running", Message: "collecting pod logs", ObservedGeneration: pipeline.Generation, LastTransitionTime: &now}
	if err := r.Status().Update(ctx, &pipeline); err != nil {
		cancel()
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *LogPipelineReconciler) stop(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.active[key]; ok {
		current.cancel()
		delete(r.active, key)
	}
}
func (r *LogPipelineReconciler) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, current := range r.active {
		current.cancel()
		delete(r.active, key)
	}
}
func (r *LogPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&observabilityv1alpha1.LogPipeline{}).Complete(r)
}
