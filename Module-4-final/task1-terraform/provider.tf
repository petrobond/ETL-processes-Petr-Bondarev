terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
      version = ">= 0.130"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  token     = "y0__xCg8PkMGMHdEyDEzZrkFTCUicGQCD693xlaPkR7tdvy_Ej_aQ_oUM-6"
  cloud_id  = "b1gksr2m9mcfhrbof7nf"
  folder_id = "b1ghq0b0cq7h3uui6dj1"
  zone      = "ru-central1-a"
}