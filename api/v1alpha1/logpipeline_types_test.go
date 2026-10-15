package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLogPipelineDefaults(t *testing.T) {
	p := &LogPipeline{}
	p.Default()
	if p.Spec.TargetNamespace != "influxdb3" || p.Spec.Database != "logs" || p.Spec.BatchSize != 500 || p.Spec.FlushInterval != "1s" {
		t.Fatalf("bad defaults: %#v", p.Spec)
	}
}

func TestDeepCopyIsIndependent(t *testing.T) {
	p := &LogPipeline{Spec: LogPipelineSpec{PodSelector: map[string]string{"app": "db"}}}
	cp := p.DeepCopy()
	cp.Spec.PodSelector["app"] = "other"
	if p.Spec.PodSelector["app"] != "db" {
		t.Fatal("deep copy shared map")
	}
}

func TestStatusNeverSerializesToken(t *testing.T) {
	p := LogPipeline{Status: LogPipelineStatus{Phase: "Running", Message: "active"}}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(b)), "token") {
		t.Fatalf("status exposed token field: %s", b)
	}
}
