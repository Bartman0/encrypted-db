ui = true

disable_mlock = true

api_addr = "https://127.0.0.1:8200"

storage "file" {
  path = "vault-data"
}

listener "tcp" {
  address = "0.0.0.0:8200"
  tls_cert_file = "/usr/local/etc/pki/issued/127.0.0.1.crt"
  tls_key_file  = "/usr/local/etc/pki/private/127.0.0.1.key"
}

# Enable transit secrets engine
path "sys/mounts/transit" {
  capabilities = [ "create", "read", "update", "delete", "list" ]
}

# To read enabled secrets engines
path "sys/mounts" {
  capabilities = [ "read" ]
}

