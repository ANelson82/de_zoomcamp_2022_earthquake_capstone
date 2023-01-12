resource "google_bigquery_dataset" "raw_usgs_earthquakes" {
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "OWNER"
    user_by_email = "YOUR-EMAIL@gmail.com"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  dataset_id                 = "raw_usgs_earthquakes"
  delete_contents_on_destroy = false
  location                   = var.region
  project                    = var.project
}
# terraform import google_bigquery_dataset.raw_usgs_earthquakes projects/YOUR-PROJECT-123456/datasets/raw_usgs_earthquakes
