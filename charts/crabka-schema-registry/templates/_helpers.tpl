{{- define "crabka-schema-registry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crabka-schema-registry.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "crabka-schema-registry.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "crabka-schema-registry.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "crabka-schema-registry.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "crabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: schema-registry
{{- end -}}

{{- define "crabka-schema-registry.selectorLabels" -}}
app.kubernetes.io/name: {{ include "crabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
