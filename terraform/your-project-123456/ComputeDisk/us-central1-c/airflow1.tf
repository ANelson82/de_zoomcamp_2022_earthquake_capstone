resource "google_compute_disk" "airflow1" {
  image                     = "https://www.googleapis.com/compute/beta/projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20221206"
  name                      = "airflow1"
  physical_block_size_bytes = 4096
  project                   = var.project
  size                      = 50
  type                      = "pd-balanced"
  zone                      = var.compute_region
}

