# Infrastructure for setting up integration between the Yandex Data Processing and Managed Service for Apache Kafka® clusters
#
# RU: https://yandex.cloud/ru/docs/data-proc/tutorials/kafka
# EN: https://yandex.cloud/en/docs/data-proc/tutorials/kafka

# Specify the following settings:
locals {
  folder_id  = var.yc_folder_id
  dp_ssh_key = var.dp_ssh_key

  network_name          = "dataproc-network"
  subnet_name           = "dataproc-subnet-b"
  sa_name               = "dataproc-sa"
  sa_bucket             = "bucket-sa"
  bucket_name           = var.bucket_name
  dataproc_cluster_name = "dataproc-cluster"
  kafka_cluster_name    = "dataproc-kafka"
  kafka_username        = "user1"
  kafka_password        = "password1"
  topic_name            = "dataproc-kafka-topic"
}

resource "yandex_vpc_network" "dataproc_network" {
  description = "Network for Yandex Data Processing and Managed Service for Apache Kafka®"
  name        = local.network_name
}

resource "yandex_vpc_subnet" "dataproc_subnet_b" {
  description    = "Subnet for Yandex Data Processing and Managed Service for Apache Kafka®"
  name           = local.subnet_name
  zone           = "ru-central1-b"
  network_id     = yandex_vpc_network.dataproc_network.id
  v4_cidr_blocks = ["10.140.0.0/24"]
}

resource "yandex_vpc_security_group" "dataproc_security_group" {
  description = "Security group for the Yandex Data Processing and Managed Service for Apache Kafka® clusters"
  network_id  = yandex_vpc_network.dataproc_network.id

  ingress {
    description       = "Allow any incoming traffic within the security group"
    protocol          = "ANY"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  ingress {
    description    = "Allow access to NTP servers for time syncing"
    protocol       = "UDP"
    port           = 123
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description       = "Allow any outgoing traffic within the security group"
    protocol          = "ANY"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  egress {
    description    = "Allow connections to the HTTPS port from any IP address"
    protocol       = "TCP"
    port           = 443
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description    = "Allow access to NTP servers for time syncing"
    protocol       = "UDP"
    port           = 123
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "yandex_iam_service_account" "dataproc_sa" {
  description = "Service account to manage the Yandex Data Processing cluster"
  name        = local.sa_name
}

resource "yandex_iam_service_account" "bucket_sa" {
  description = "Service account to manage the Object Storage bucket"
  name        = local.sa_bucket
}

# Assign the storage.admin role to the Object Storage service account
resource "yandex_resourcemanager_folder_iam_binding" "storage_admin" {
  folder_id = local.folder_id
  role      = "storage.admin"
  members   = ["serviceAccount:${yandex_iam_service_account.bucket_sa.id}"]
}

# Assign the dataproc.agent role to the Yandex Data Processing service account
resource "yandex_resourcemanager_folder_iam_binding" "dataproc_agent" {
  folder_id = local.folder_id
  role      = "dataproc.agent"
  members   = ["serviceAccount:${yandex_iam_service_account.dataproc_sa.id}"]
}

# Assign the dataproc.provisioner role to the Yandex Data Processing service account
resource "yandex_resourcemanager_folder_iam_binding" "dataproc_provisioner" {
  folder_id = local.folder_id
  role      = "dataproc.provisioner"
  members   = ["serviceAccount:${yandex_iam_service_account.dataproc_sa.id}"]
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  description        = "Static access key for Object Storage"
  service_account_id = yandex_iam_service_account.bucket_sa.id
}

# Use the key to create a bucket
resource "yandex_storage_bucket" "dataproc_bucket" {
  access_key = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  bucket     = local.bucket_name

  depends_on = [
    yandex_resourcemanager_folder_iam_binding.storage_admin
  ]

  grant {
    id          = yandex_iam_service_account.dataproc_sa.id
    type        = "CanonicalUser"
    permissions = ["READ","WRITE"]
  }
}

# Data Proc cluster will be created via yc CLI (requires NAT gateway which is not supported by provider v0.72)

# Kafka cluster + topic will be created via yc CLI (provider v0.72 has limited Kafka support)
# yc managed-kafka cluster create --name dataproc-kafka --network-id enpp83uiovmfpggvj1c0 --security-group-ids enpf5bi38g06cp5l5nnu --zone-ids ru-central1-b --brokers-count 1 --resource-preset s2.micro --disk-size 10 --disk-type network-ssd --version 3.5 --assign-public-ip
# yc managed-kafka topic create --cluster-name dataproc-kafka --name dataproc-kafka-topic --partitions 1 --replication-factor 1
# yc managed-kafka user create --cluster-name dataproc-kafka --username user1 --password password1 --permission topic_name=*,role=ACCESS_ROLE_CONSUMER --permission topic_name=*,role=ACCESS_ROLE_PRODUCER --permission topic_name=*,role=ACCESS_ROLE_ADMIN
