# Infrastructure for the Yandex Cloud Managed Service for Apache Kafka®, Managed Service for MongoDB, and Data Transfer
#
# RU: https://yandex.cloud/ru/docs/data-transfer/tutorials/mkf-to-mmg
# EN: https://yandex.cloud/en/docs/data-transfer/tutorials/mkf-to-mmg

#
# Specify the following settings:
variable "source_kf_version" {
  type        = string
  description = "Desired version of Apache Kafka®"
}

variable "source_user_password" {
  type        = string
  description = "Apache Kafka® user's password"
  sensitive   = true
}

variable "target_mg_version" {
  type        = string
  description = "Desired version of MongoDB"
}

variable "target_user_password" {
  type        = string
  description = "MongoDB user's password"
  sensitive   = true
}

locals {
  # The following settings are predefined. Change them only if necessary.
  network_name    = "network"          # Name of the network
  subnet_name     = "subnet-a"         # Name of the subnet
  kf_cluster_name = "kafka-cluster"    # Name of the Apache Kafka® cluster
  kf_username     = "mkf-user"         # Username of the Apache Kafka® cluster
  kf_topic        = "sensors"          # Name of the Apache Kafka® topic
  mg_cluster_name = "mongodb-cluster"  # Name of the MongoDB cluster
  mg_db_name      = "db1"              # Name of the MongoDB cluster database
  mg_username     = "mmg-user"         # Username of the MongoDB cluster
  transfer_name   = "mkf-mmg-transfer" # Name of the transfer from the Managed Service for Apache Kafka® to the Managed Service for MongoDB
}

# Use existing network infrastructure

data "yandex_vpc_network" "default" {
  name = "default"
}

data "yandex_vpc_subnet" "default-ru-central1-a" {
  name = "default-ru-central1-a"
}

data "yandex_vpc_security_group" "default" {
  name = "default-sg-enpgcdg4kehklvoud5v6"
}

# Add ingress rules to the default security group for Kafka and MongoDB

resource "yandex_vpc_security_group_rule" "kafka-9091" {
  security_group_binding = data.yandex_vpc_security_group.default.id
  description            = "Allow connections to Managed Service for Apache Kafka from the Internet"
  direction              = "ingress"
  protocol               = "TCP"
  port                   = 9091
  v4_cidr_blocks         = ["0.0.0.0/0"]
}

resource "yandex_vpc_security_group_rule" "mongodb-27018" {
  security_group_binding = data.yandex_vpc_security_group.default.id
  description            = "Allow connections to Managed Service for MongoDB from the Internet"
  direction              = "ingress"
  protocol               = "TCP"
  port                   = 27018
  v4_cidr_blocks         = ["0.0.0.0/0"]
}

# Infrastructure for the Managed Service for Apache Kafka® cluster

resource "yandex_mdb_kafka_cluster" "kafka-cluster" {
  description        = "Managed Service for Apache Kafka® cluster"
  name               = local.kf_cluster_name
  environment        = "PRODUCTION"
  network_id         = data.yandex_vpc_network.default.id
  security_group_ids = [data.yandex_vpc_security_group.default.id]

  config {
    brokers_count    = 1
    version          = var.source_kf_version
    zones            = ["ru-central1-a"]
    assign_public_ip = true # Required for connection from the Internet
    kafka {
      resources {
        resource_preset_id = "s2.micro" # 2 vCPU, 8 GB RAM
        disk_type_id       = "network-hdd"
        disk_size          = 10 # GB
      }
    }
  }
}

# Topic of the Managed Service for Apache Kafka® cluster
resource "yandex_mdb_kafka_topic" "sensors" {
  cluster_id         = yandex_mdb_kafka_cluster.kafka-cluster.id
  name               = local.kf_topic
  partitions         = 2
  replication_factor = 1
}

# User of the Managed Service for Apache Kafka® cluster
resource "yandex_mdb_kafka_user" "mkf-user" {
  cluster_id = yandex_mdb_kafka_cluster.kafka-cluster.id
  name       = local.kf_username
  password   = var.source_user_password
  permission {
    topic_name = yandex_mdb_kafka_topic.sensors.name
    role       = "ACCESS_ROLE_CONSUMER"
  }
  permission {
    topic_name = yandex_mdb_kafka_topic.sensors.name
    role       = "ACCESS_ROLE_PRODUCER"
  }
}

# Infrastructure for the Managed Service for MongoDB cluster

resource "yandex_mdb_mongodb_cluster" "mongodb-cluster" {
  description        = "Managed Service for MongoDB cluster"
  name               = local.mg_cluster_name
  environment        = "PRODUCTION"
  network_id         = data.yandex_vpc_network.default.id
  security_group_ids = [data.yandex_vpc_security_group.default.id]

  cluster_config {
    version = var.target_mg_version
  }

  resources_mongod {
    resource_preset_id = "s2.micro" # 2 vCPU, 8 GB RAM
    disk_type_id       = "network-hdd"
    disk_size          = 10 # GB
  }

  host {
    zone_id          = "ru-central1-a"
    subnet_id        = data.yandex_vpc_subnet.default-ru-central1-a.id
    assign_public_ip = true # Required for connection from the Internet
  }
}

# Database of the Managed Service for MongoDB cluster
resource "yandex_mdb_mongodb_database" "db1" {
  cluster_id = yandex_mdb_mongodb_cluster.mongodb-cluster.id
  name       = local.mg_db_name
}

# User of the Managed Service for MongoDB cluster
resource "yandex_mdb_mongodb_user" "mmg-user" {
  cluster_id = yandex_mdb_mongodb_cluster.mongodb-cluster.id
  name       = local.mg_username
  password   = var.target_user_password
  permission {
    database_name = yandex_mdb_mongodb_database.db1.name
    roles         = ["readWrite"]
  }
}

# Data Transfer infrastructure

resource "yandex_datatransfer_endpoint" "kafka_source" {
  name = "kf-source"
  settings {
    kafka_source {
      connection {
        cluster_id = yandex_mdb_kafka_cluster.kafka-cluster.id
      }
      auth {
        sasl {
          user      = local.kf_username
          mechanism = "KAFKA_MECHANISM_SHA512"
          password {
            raw = var.source_user_password
          }
        }
      }
      topic_names = [local.kf_topic]
      parser {
        json_parser {
          add_rest_column  = true
          null_keys_allowed = true
          data_schema {
            fields {
              fields {
                name = "device_id"
                type = "STRING"
              }
            }
          }
        }
      }
    }
  }
}

resource "yandex_datatransfer_endpoint" "mongodb_target" {
  name = "mg-target"
  settings {
    mongo_target {
      connection {
        connection_options {
          mdb_cluster_id = yandex_mdb_mongodb_cluster.mongodb-cluster.id
          user           = local.mg_username
          password {
            raw = var.target_user_password
          }
          auth_source = local.mg_db_name
        }
      }
      database       = local.mg_db_name
      cleanup_policy = "DISABLED"
    }
  }
}

resource "yandex_datatransfer_transfer" "mkf-mmg-transfer" {
  description = "Transfer from the Managed Service for Apache Kafka® to the Managed Service for MongoDB"
  name        = local.transfer_name
  source_id   = yandex_datatransfer_endpoint.kafka_source.id
  target_id   = yandex_datatransfer_endpoint.mongodb_target.id
  type        = "INCREMENT_ONLY" # Replication data
}