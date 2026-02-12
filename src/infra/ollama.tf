resource "kubernetes_namespace" "daybook" {
  metadata {
    name = "${var.env_slug}-daybook"
  }
}

// add again
resource "helm_release" "ollama" {
  depends_on = [kubernetes_namespace.daybook]
  namespace = kubernetes_namespace.daybook.metadata[0].name
  name      = "ollama"
  atomic    = true

  repository = "https://helm.otwld.com/"
  chart = "ollama"
  version = "1.42.0"


  values = [
    yamlencode({
      image = {
        repository = "alpine/ollama"
      }
      ollama = {
        models = {
          pull = [
            "gemma3",
            "embeddinggemma"
          ]
        }
      }
      persistentVolume = {
        enabled = true
        size = "20Gi"
      }
    })
  ]
}

resource "kubernetes_manifest" "ollama_route" {
  depends_on = [helm_release.ollama]
  manifest = {
    apiVersion = "gateway.networking.k8s.io/v1"
    kind       = "HTTPRoute"
    metadata = {
      name      = "ollama-route"
      namespace = kubernetes_namespace.daybook.metadata[0].name
    }
    spec = {
      parentRefs = [
        {
          name = "main-gateway"
          namespace = "envoy-gateway"
        }
      ]
      hostnames = [
        "ollama${data.infisical_secrets.root.secrets["HOST_DOMAIN"].value}",
      ]
      rules = [
        {
          matches = [
            {
              path = {
                type  = "PathPrefix"
                value = "/"
              }
            }
          ]
          backendRefs = [
            {
              namespace = kubernetes_namespace.daybook.metadata[0].name
              name = helm_release.ollama.name
              port = 11434
            }
          ]
        }
      ]
    }
  }
}

resource "kubernetes_secret" "ollama_basic_auth" {
  depends_on = [helm_release.ollama]
  metadata {
    name = "ollama-api-auth"
    namespace = kubernetes_namespace.daybook.metadata[0].name
  }
  data = {
    ".htpasswd" = data.infisical_secrets.root.secrets["OLLAMA_AUTH_BASIC_HTPASSWD"].value
  }
}

resource "kubernetes_manifest" "ollama_sec_policy" {
  depends_on = [helm_release.ollama]
  manifest = {
    apiVersion = "gateway.envoyproxy.io/v1alpha1"
    kind       = "SecurityPolicy"
    metadata = {
      name      = "ollama-basic-auth-policy"
      namespace = kubernetes_namespace.daybook.metadata[0].name
    }
    spec = {
      targetRefs = [
        {
          group = "gateway.networking.k8s.io"
          kind = "HTTPRoute"
          name = kubernetes_manifest.ollama_route.manifest.metadata.name
        }
      ]
      basicAuth = {
        users = {
          name = kubernetes_secret.ollama_basic_auth.metadata[0].name
        }
      }
      /*apiKeyAuth = {
        credentialRefs = [
          {
            group = ""
            kind = "Secret"
            name = "ollama-api-key"
            namespace = kubernetes_namespace.daybook.metadata[0].name
          }
        ]
        extractFrom = [
          {
            headers = [
              "x-api-key"
            ]
          }
        ]
      }*/
    }
  }
}
