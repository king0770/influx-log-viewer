package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	observabilityv1alpha1 "github.com/influxdata/influx-log-viewer/api/v1alpha1"
	"github.com/influxdata/influx-log-viewer/internal/collector"
	pipelinecontroller "github.com/influxdata/influx-log-viewer/internal/controller"
	"github.com/influxdata/influx-log-viewer/internal/influx"
	"github.com/influxdata/influx-log-viewer/internal/web"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = observabilityv1alpha1.AddToScheme(s)
	return s
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	if err := serve(ctrl.SetupSignalHandler(), cfg); err != nil {
		log.Fatal(err)
	}
}

func serve(ctx context.Context, cfg config) error {
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes config: %w", err)
	}
	scheme := newScheme()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", LeaderElection: false})
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	hub := web.NewHub(2000)
	searchClient, err := influx.NewClient(cfg.QueryURL, cfg.Token, cfg.Database, nil)
	if err != nil {
		return err
	}
	factory := func(p *observabilityv1alpha1.LogPipeline) pipelinecontroller.Runner {
		influxURL := p.Spec.InfluxURL
		if influxURL == "" {
			influxURL = cfg.InfluxURL
		}
		db := p.Spec.Database
		if db == "" {
			db = cfg.Database
		}
		writer, werr := influx.NewClient(influxURL, cfg.Token, db, nil)
		if werr != nil {
			return errorRunner{werr}
		}
		interval, _ := time.ParseDuration(p.Spec.FlushInterval)
		selector := labels.SelectorFromSet(p.Spec.PodSelector).String()
		return collector.New(collector.KubeSource{Client: clientset}, writer, collector.Config{Namespace: p.Spec.TargetNamespace, LabelSelector: selector, BatchSize: p.Spec.BatchSize, FlushInterval: interval}, hub.Publish)
	}
	reconciler := pipelinecontroller.NewLogPipelineReconciler(mgr.GetClient(), scheme, factory)
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return err
	}
	defer reconciler.StopAll()
	var ready atomic.Bool
	api := web.NewServer(searchClient, hub, ready.Load)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 2)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errs <- err
		}
	}()
	go func() {
		ready.Store(true)
		if err := mgr.Start(ctx); err != nil {
			errs <- err
		}
	}()
	select {
	case <-ctx.Done():
		ready.Store(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errs:
		return err
	}
}

type errorRunner struct{ err error }

func (e errorRunner) Run(context.Context) error { return e.err }
