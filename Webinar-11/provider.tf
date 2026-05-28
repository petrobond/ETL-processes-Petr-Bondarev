terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.129"
    }
  }
  required_version = ">= 1.0"
}

provider "yandex" {
  cloud_id  = "b1gksr2m9mcfhrbof7nf"
  folder_id = "b1ghq0b0cq7h3uui6dj1"
  zone      = "ru-central1-a"
}
