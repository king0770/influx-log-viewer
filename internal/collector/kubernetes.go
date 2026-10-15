package collector

import (
	"context"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type KubeSource struct{ Client kubernetes.Interface }

func (k KubeSource) List(ctx context.Context, namespace, selector string) ([]Target, error) {
	pods, err := k.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	targets := make([]Target, 0)
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" {
			continue
		}
		for _, container := range pod.Spec.Containers {
			targets = append(targets, Target{Namespace: namespace, Pod: pod.Name, Container: container.Name})
		}
	}
	return targets, nil
}

func (k KubeSource) Follow(ctx context.Context, target Target, since time.Time) (io.ReadCloser, error) {
	opts := &corev1.PodLogOptions{Container: target.Container, Follow: true, Timestamps: true}
	if !since.IsZero() {
		t := metav1.NewTime(since)
		opts.SinceTime = &t
	}
	return k.Client.CoreV1().Pods(target.Namespace).GetLogs(target.Pod, opts).Stream(ctx)
}
