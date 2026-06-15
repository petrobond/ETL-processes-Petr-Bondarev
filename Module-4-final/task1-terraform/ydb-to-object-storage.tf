locals {
  folder_id   = "b1ghq0b0cq7h3uui6dj1"
  bucket_name = "petr-bondarev-module4-task1"

  ydb_database_id   = "etngn7jkcmpvd81u9p9j"
  dp_sa_id        = "aje1q0iq187g4q90g9ku"
  dp_sa_name      = "sa-for-transfer"
  ydb_endpoint      = "grpcs://ydb.serverless.yandexcloud.net:2135"
  ydb_database_path = "/ru-central1/b1gksr2m9mcfhrbof7nf/${local.ydb_database_id}"

  yt_endpoint_name = "ydb-source-tf"
  obj_endpoint_name = "obj-storage-target-tf"
  dt_transfer_name  = "ydb-to-obj-storage-transfer"
  dt_enabled        = 1
}

# Data sources for existing resources
data "yandex_iam_service_account" "sa-for-transfer" {
  name = local.dp_sa_name
}

# Source endpoint: YDB
resource "yandex_datatransfer_endpoint" "ydb-source" {
  name        = local.yt_endpoint_name
  description = "Source: YDB database for Data Transfer"
  folder_id   = local.folder_id

  settings {
    ydb_source {
      database           = local.ydb_database_path
      instance           = local.ydb_endpoint
      service_account_id = data.yandex_iam_service_account.sa-for-transfer.id
    }
  }
}

# Target endpoint: Object Storage
resource "yandex_datatransfer_endpoint" "obj-storage-target" {
  name        = local.obj_endpoint_name
  description = "Target: Object Storage bucket"
  folder_id   = local.folder_id

  settings {
    object_storage_target {
      bucket = local.bucket_name
    }
  }
}

# Transfer: YDB → Object Storage
resource "yandex_datatransfer_transfer" "ydb-to-obj-storage" {
  count       = local.dt_enabled
  description = "Transfer YDB → Object Storage (snapshot)"
  name        = local.dt_transfer_name
  source_id   = yandex_datatransfer_endpoint.ydb-source.id
  target_id   = yandex_datatransfer_endpoint.obj-storage-target.id
  type        = "SNAPSHOT_ONLY"
}