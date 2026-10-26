{{- define "telemetry-job.name" -}}
{{- required "job is required" .Values.job | trunc 52 | trimSuffix "-" -}}
{{- end -}}

{{- define "telemetry-job.labels" -}}
app.kubernetes.io/name: {{ include "telemetry-job.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "telemetry-job.selector" -}}
app.kubernetes.io/name: {{ include "telemetry-job.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
