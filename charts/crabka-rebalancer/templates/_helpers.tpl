{{- define "rebalancer.name" -}}
crabka-rebalancer
{{- end -}}

{{- define "rebalancer.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "rebalancer.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rebalancer.labels" -}}
app.kubernetes.io/name: {{ include "rebalancer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "rebalancer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rebalancer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rebalancer.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
