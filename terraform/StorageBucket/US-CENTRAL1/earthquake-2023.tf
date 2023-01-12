resource "google_storage_bucket" "earthquake_2023" {
  force_destroy               = false
  location                    = var.region
  name                        = "earthquake-2023"
  project                     = var.project
  public_access_prevention    = "enforced"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}
# terraform import google_storage_bucket.earthquake_2023 earthquake-2023
