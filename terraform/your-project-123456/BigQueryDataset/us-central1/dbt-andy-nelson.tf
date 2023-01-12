resource "google_bigquery_dataset" "dbt_andy_nelson" {
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

  dataset_id                 = "dbt_andy_nelson"
  delete_contents_on_destroy = false
  location                   = var.region
  project                    = var.project
}
# terraform import google_bigquery_dataset.dbt_andy_nelson projects/YOUR-PROJECT-123456/datasets/dbt_andy_nelson
