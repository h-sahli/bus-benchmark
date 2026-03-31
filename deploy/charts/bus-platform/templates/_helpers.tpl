{{- define "bus-platform.name" -}}
bus-platform
{{- end -}}

{{- define "bus-platform.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace -}}
{{- end -}}

{{- define "bus-platform.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
{{- else -}}
{{- include "bus-platform.name" . -}}
{{- end -}}
{{- end -}}

{{- define "bus-platform.platformImage" -}}
{{ printf "%s:%s" .Values.images.platform.repository .Values.images.platform.tag }}
{{- end -}}

{{- define "bus-platform.agentImage" -}}
{{ printf "%s:%s" .Values.images.agent.repository .Values.images.agent.tag }}
{{- end -}}

{{- define "bus-platform.ingressHostMode" -}}
{{- $mode := default "auto" .Values.ingress.hostMode | lower -}}
{{- if or (eq $mode "auto") (eq $mode "manual") (eq $mode "hostless") -}}
{{- $mode -}}
{{- else -}}
auto
{{- end -}}
{{- end -}}

{{- define "bus-platform.ingressBootstrapHost" -}}
{{- default "bus-platform-pending.invalid" .Values.ingress.bootstrapHost -}}
{{- end -}}

{{- define "bus-platform.ingressHost" -}}
{{- $mode := include "bus-platform.ingressHostMode" . -}}
{{- if eq $mode "hostless" -}}
{{- "" -}}
{{- else if .Values.ingress.host -}}
{{- .Values.ingress.host -}}
{{- else -}}
{{- include "bus-platform.ingressBootstrapHost" . -}}
{{- end -}}
{{- end -}}

{{- define "bus-platform.ingressHasFinalHost" -}}
{{- $host := include "bus-platform.ingressHost" . -}}
{{- and $host (ne $host (include "bus-platform.ingressBootstrapHost" .)) -}}
{{- end -}}

{{- define "bus-platform.ingressUrl" -}}
{{- $host := include "bus-platform.ingressHost" . -}}
{{- if $host -}}
http{{ if .Values.ingress.tls.enabled }}s{{ end }}://{{ $host }}{{ .Values.ingress.path }}
{{- end -}}
{{- end -}}

{{- define "bus-platform.nodePortUrl" -}}
{{- if and (eq .Values.service.type "NodePort") .Values.service.nodePort -}}
http://{{ default "localhost" .Values.service.localAccessHost }}:{{ .Values.service.nodePort }}/
{{- end -}}
{{- end -}}

{{- define "bus-platform.accessUrl" -}}
{{- if .Values.env.accessUrl -}}
{{- .Values.env.accessUrl -}}
{{- else if and .Values.ingress.enabled (eq (include "bus-platform.ingressHasFinalHost" .) "true") -}}
{{- include "bus-platform.ingressUrl" . -}}
{{- else if eq .Values.service.type "NodePort" -}}
{{- include "bus-platform.nodePortUrl" . -}}
{{- end -}}
{{- end -}}
