# Data Proc cluster
resource "yandex_dataproc_cluster" "dataproc_cluster" {
  description        = "Yandex Data Processing cluster"
  bucket             = "petr-bondarev-module4-task1"
  security_group_ids = ["enpnkb1b5vj89b8q2s34"]
  name               = "dataproc-cluster"
  service_account_id = "ajecat45o7378irkchsb"
  zone_id            = "ru-central1-b"
  ui_proxy           = true

  cluster_config {
    version_id = "2.1"

    hadoop {
      services        = ["HDFS", "LIVY", "SPARK", "TEZ", "YARN"]
      ssh_public_keys = [file("/Users/Lenovo/.ssh/id_rsa_dataproc.pub")]
    }

    subcluster_spec {
      name = "main"
      role = "MASTERNODE"
      resources {
        resource_preset_id = "s2.micro"
        disk_type_id       = "network-hdd"
        disk_size          = 20
      }
      subnet_id   = "e2ls9japvi3p6onlp9fe"
      hosts_count = 1
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = "s2.micro"
        disk_type_id       = "network-hdd"
        disk_size          = 20
      }
      subnet_id   = "e2ls9japvi3p6onlp9fe"
      hosts_count = 1
    }
  }
}