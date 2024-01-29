variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "peerless-sensor-411315"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "region" {
  description = "Region"
  default     = "europe-central2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "de_zc_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "peerless-sensor-411315-terrabucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}