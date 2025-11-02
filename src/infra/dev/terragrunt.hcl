include {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = ".."
}

inputs = {
  env_slug = "dev"
}
