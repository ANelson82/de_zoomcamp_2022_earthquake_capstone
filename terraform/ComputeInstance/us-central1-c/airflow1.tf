resource "google_compute_instance" "airflow1" {
  boot_disk {
    auto_delete = true
    device_name = "airflow"

    initialize_params {
      image = "https://www.googleapis.com/compute/beta/projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20221206"
      size  = 50
      type  = "pd-balanced"
    }

    mode   = "READ_WRITE"
    source = "https://www.googleapis.com/compute/v1/projects/YOUR-PROJECT-123456/zones/us-central1-c/disks/airflow1"
  }

  confidential_instance_config {
    enable_confidential_compute = false
  }

  machine_type = "e2-standard-4"
  name         = "airflow1"

  project = var.project

  reservation_affinity {
    type = "ANY_RESERVATION"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    provisioning_model  = "STANDARD"
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_vtpm                 = true
  }

  tags = ["http-server", "https-server"]
  zone = var.compute_region
}

