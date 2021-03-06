# Grant permissions on user specific path
path "user-kv/data/{{identity.entity.name}}/*" {
    capabilities = [ "create", "update", "read", "delete", "list" ]
}

# For Web UI usage
path "user-kv/metadata" {
  capabilities = ["list"]
}

# Manage the transit secrets engine
path "transit-{{identity.entity.name}}/*" {
  capabilities = [ "create", "read", "update", "delete", "list" ]
}

