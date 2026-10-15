// Package v1alpha1 contains the observability.influxdata.com API.
// +kubebuilder:object:generate=true
// +groupName=observability.influxdata.com
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "observability.influxdata.com", Version: "v1alpha1"}
var SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &LogPipeline{}, &LogPipelineList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
})
var AddToScheme = SchemeBuilder.AddToScheme

type LogPipelineSpec struct {
	TargetNamespace string            `json:"targetNamespace,omitempty"`
	PodSelector     map[string]string `json:"podSelector,omitempty"`
	InfluxURL       string            `json:"influxURL,omitempty"`
	Database        string            `json:"database,omitempty"`
	BatchSize       int               `json:"batchSize,omitempty"`
	FlushInterval   string            `json:"flushInterval,omitempty"`
}

type LogPipelineStatus struct {
	Phase              string       `json:"phase,omitempty"`
	Message            string       `json:"message,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=lp
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
type LogPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LogPipelineSpec   `json:"spec,omitempty"`
	Status            LogPipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type LogPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogPipeline `json:"items"`
}

func (p *LogPipeline) Default() {
	if p.Spec.TargetNamespace == "" {
		p.Spec.TargetNamespace = "influxdb3"
	}
	if p.Spec.Database == "" {
		p.Spec.Database = "logs"
	}
	if p.Spec.BatchSize <= 0 {
		p.Spec.BatchSize = 500
	}
	if p.Spec.FlushInterval == "" {
		p.Spec.FlushInterval = "1s"
	}
}

func (in *LogPipeline) DeepCopyInto(out *LogPipeline) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if in.Spec.PodSelector != nil {
		out.Spec.PodSelector = make(map[string]string, len(in.Spec.PodSelector))
		for k, v := range in.Spec.PodSelector {
			out.Spec.PodSelector[k] = v
		}
	}
	if in.Status.LastTransitionTime != nil {
		out.Status.LastTransitionTime = in.Status.LastTransitionTime.DeepCopy()
	}
}
func (in *LogPipeline) DeepCopy() *LogPipeline {
	if in == nil {
		return nil
	}
	out := new(LogPipeline)
	in.DeepCopyInto(out)
	return out
}
func (in *LogPipeline) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *LogPipelineList) DeepCopyInto(out *LogPipelineList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]LogPipeline, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *LogPipelineList) DeepCopy() *LogPipelineList {
	if in == nil {
		return nil
	}
	out := new(LogPipelineList)
	in.DeepCopyInto(out)
	return out
}
func (in *LogPipelineList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
