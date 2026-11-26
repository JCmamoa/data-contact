variable "aws_region" {
  description = "AWS region for the Cloud Mirror infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "project_slug" {
  description = "Short project identifier used in resource naming."
  type        = string
  default     = "dcsp"
}

variable "environment" {
  description = "Deployment environment (development, staging, production)."
  type        = string
  default     = "development"

  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "environment must be one of: development, staging, production."
  }
}

variable "quarantine_retention_days" {
  description = "Days before quarantined dead-letter batches expire."
  type        = number
  default     = 180
}

variable "athena_scan_cutoff_bytes" {
  description = "Maximum bytes scanned per Athena query to enforce FinOps guardrails (default 10 GB)."
  type        = number
  default     = 10737418240
}

variable "redshift_db_name" {
  description = "Default Redshift Serverless database name."
  type        = string
  default     = "analytics_dw"
}

variable "redshift_admin_username" {
  description = "Admin username for Redshift Serverless namespace (password managed via Secrets Manager)."
  type        = string
  default     = "analytics_admin"
}

variable "redshift_base_rpus" {
  description = "Base Redshift Processing Units (RPUs) for serverless workgroup."
  type        = number
  default     = 8
}

variable "alert_email" {
  description = "Optional email endpoint subscribed to the Data Contract SNS alert topic."
  type        = string
  default     = ""
}
