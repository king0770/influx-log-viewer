package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	observabilityv1alpha1 "github.com/influxdata/influx-log-viewer/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type runnerFunc func(context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

func TestReconcileDefaultsStartsAndUpdatesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = observabilityv1alpha1.AddToScheme(scheme)
	pipeline := &observabilityv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "ops", Generation: 1}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(pipeline).WithObjects(pipeline).Build()
	started := make(chan observabilityv1alpha1.LogPipelineSpec, 1)
	r := NewLogPipelineReconciler(cl, scheme, func(p *observabilityv1alpha1.LogPipeline) Runner {
		started <- p.Spec
		return runnerFunc(func(ctx context.Context) error { <-ctx.Done(); return nil })
	})
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "main", Namespace: "ops"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case spec := <-started:
		if spec.TargetNamespace != "influxdb3" || spec.Database != "logs" {
			t.Fatalf("bad defaults %#v", spec)
		}
	case <-time.After(time.Second):
		t.Fatal("runner not started")
	}
	var got observabilityv1alpha1.LogPipeline
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "main", Namespace: "ops"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Running" || got.Status.ObservedGeneration != 1 {
		t.Fatalf("bad status %#v", got.Status)
	}
	r.StopAll()
}

func TestReconcileRestartCancelsOldGeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = observabilityv1alpha1.AddToScheme(scheme)
	p := &observabilityv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", Generation: 1}, Spec: observabilityv1alpha1.LogPipelineSpec{Database: "logs"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(p).WithObjects(p).Build()
	var cancelled atomic.Int32
	r := NewLogPipelineReconciler(cl, scheme, func(*observabilityv1alpha1.LogPipeline) Runner {
		return runnerFunc(func(ctx context.Context) error { <-ctx.Done(); cancelled.Add(1); return nil })
	})
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "p", Namespace: "n"}}
	_, _ = r.Reconcile(context.Background(), req)
	var updated observabilityv1alpha1.LogPipeline
	_ = cl.Get(context.Background(), req.NamespacedName, &updated)
	updated.Generation = 2
	_ = cl.Update(context.Background(), &updated)
	_, _ = r.Reconcile(context.Background(), req)
	deadline := time.Now().Add(time.Second)
	for cancelled.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cancelled.Load() == 0 {
		t.Fatal("old runner not cancelled")
	}
	r.StopAll()
}
