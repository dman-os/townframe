remote_state {
  backend = "kubernetes" 
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }
  config = {
    config_path = "./kubeconfig.yaml"
    secret_suffix = "daybook-${replace(basename(path_relative_to_include()), "/[/ _]/", "-")}"
    namespace = "terraform-state"
  }
}

locals {
  infisical_json = jsondecode(file("${get_repo_root()}/.infisical.json"))
  infisical_workspace_id = local.infisical_json["workspaceId"]
}

inputs = {
  infisical_workspace_id  = local.infisical_json["workspaceId"]
}


generate "providers" {
  path      = "providers.tf"
  if_exists = "overwrite"
  contents  = <<EOF
terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.38.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0.2"
    }
    infisical = {
      version = "~> 0.15.40"
      source = "infisical/infisical"
    }
  }
}

provider "kubernetes" {
  config_path = "./kubeconfig.yaml"
}

provider "helm" {
  kubernetes = {
    config_path = "./kubeconfig.yaml"
  }
}

provider "infisical" {
  auth = {
    universal = {
    }
  }
}

data "infisical_secrets" "root" {
  env_slug     = "prd"
  workspace_id = "${local.infisical_workspace_id}"
  folder_path  = "/"
}

EOF
}

generate "kubeconfig" {
  path      = "kubeconfig.yaml"
  if_exists = "overwrite"
  contents  = get_env("KUBECONFIG_YML")
}
