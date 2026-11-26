# ==============================================================================
# AWS Cloud Mirror — Data Contracts & Semantic Platform
# Mirrors local stack (SQLite/Postgres + Quarantine + dbt + Alerts) to AWS:
#   - S3 Raw, Quarantine & Curated Data Lake Zones
#   - AWS Glue Data Catalog (raw, staging, marts, semantic databases)
#   - Amazon Redshift Serverless & Amazon Athena Workgroup
#   - Amazon SNS Topic for Shift-Left Data Contract Violation Alerts
# ==============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_slug
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "data-contracts-semantic-platform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project_slug}-${var.environment}"
  account_id  = data.aws_caller_identity.current.account_id

  lake_buckets = {
    raw        = "${local.name_prefix}-raw-${local.account_id}"
    quarantine = "${local.name_prefix}-quarantine-${local.account_id}"
    curated    = "${local.name_prefix}-curated-${local.account_id}"
    athena     = "${local.name_prefix}-athena-results-${local.account_id}"
  }

  glue_databases = {
    raw      = "${replace(var.project_slug, "-", "_")}_raw_${var.environment}"
    staging  = "${replace(var.project_slug, "-", "_")}_staging_${var.environment}"
    marts    = "${replace(var.project_slug, "-", "_")}_marts_${var.environment}"
    semantic = "${replace(var.project_slug, "-", "_")}_semantic_${var.environment}"
  }
}

# ------------------------------------------------------------------------------
# 1. KMS Key for Data Lake, Quarantine, SNS & Redshift Encryption
# ------------------------------------------------------------------------------
resource "aws_kms_key" "platform" {
  description             = "KMS CMK for ${local.name_prefix} data lake buckets, SNS alerts, and warehouse"
  deletion_window_in_days = 14
  enable_key_rotation     = true
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${local.name_prefix}-platform"
  target_key_id = aws_kms_key.platform.key_id
}

# ------------------------------------------------------------------------------
# 2. S3 Buckets: Raw Landing, Quarantine Dead-Letter, Curated Lakehouse & Athena
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "zones" {
  for_each = local.lake_buckets

  bucket        = each.value
  force_destroy = var.environment != "production"
}

resource "aws_s3_bucket_versioning" "zones" {
  for_each = aws_s3_bucket.zones

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zones" {
  for_each = aws_s3_bucket.zones

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.platform.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "zones" {
  for_each = aws_s3_bucket.zones

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "quarantine_retention" {
  bucket = aws_s3_bucket.zones["quarantine"].id

  rule {
    id     = "quarantine-cold-tiering"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = var.quarantine_retention_days
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_retention" {
  bucket = aws_s3_bucket.zones["raw"].id

  rule {
    id     = "raw-glacier-archival"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# ------------------------------------------------------------------------------
# 3. AWS Glue Data Catalog (Medallion & Semantic Databases)
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "medallion" {
  for_each = local.glue_databases

  name        = each.value
  description = "Glue Data Catalog database for ${each.key} layer (${var.environment})"
  location_uri = (
    each.key == "raw"
    ? "s3://${aws_s3_bucket.zones["raw"].bucket}/"
    : "s3://${aws_s3_bucket.zones["curated"].bucket}/${each.key}/"
  )
}

# ------------------------------------------------------------------------------
# 4. Amazon Athena Workgroup (Federated Lakehouse & Semantic Verification)
# ------------------------------------------------------------------------------
resource "aws_athena_workgroup" "semantic_analytics" {
  name        = "${local.name_prefix}-semantic-wg"
  description = "Workgroup for querying Glue Iceberg/Parquet marts and validating semantic SQL"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_cutoff_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.zones["athena"].bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.platform.arn
      }
    }
  }
}

# ------------------------------------------------------------------------------
# 5. Amazon Redshift Serverless (Cloud Mirror of Local Postgres DW + dbt Marts)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "redshift_serverless" {
  name = "${local.name_prefix}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "redshift_s3_glue" {
  name = "${local.name_prefix}-redshift-s3-glue"
  role = aws_iam_role.redshift_serverless.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadCuratedAndRawBuckets"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.zones["raw"].arn,
          "${aws_s3_bucket.zones["raw"].arn}/*",
          aws_s3_bucket.zones["curated"].arn,
          "${aws_s3_bucket.zones["curated"].arn}/*"
        ]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      },
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.platform.arn
      }
    ]
  })
}

resource "aws_redshiftserverless_namespace" "analytics_dw" {
  namespace_name      = "${local.name_prefix}-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_username
  manage_admin_password = true
  kms_key_id          = aws_kms_key.platform.arn
  iam_roles           = [aws_iam_role.redshift_serverless.arn]
  default_iam_role_arn = aws_iam_role.redshift_serverless.arn
}

resource "aws_redshiftserverless_workgroup" "analytics_dw" {
  namespace_name      = aws_redshiftserverless_namespace.analytics_dw.namespace_name
  workgroup_name      = "${local.name_prefix}-wg"
  base_capacity       = var.redshift_base_rpus
  publicly_accessible = false
}

# ------------------------------------------------------------------------------
# 6. Amazon SNS Topic for Shift-Left Data Contract Violation Alerts
# ------------------------------------------------------------------------------
resource "aws_sns_topic" "data_contract_alerts" {
  name              = "${local.name_prefix}-contract-alerts"
  kms_master_key_id = aws_kms_key.platform.id
}

resource "aws_sns_topic_subscription" "contract_alert_email" {
  count = var.alert_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.data_contract_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ------------------------------------------------------------------------------
# 7. Pipeline Execution IAM Role (Ingestion -> Quarantine -> SNS -> Catalog)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "pipeline_runner" {
  name = "${local.name_prefix}-pipeline-runner"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "ecs-tasks.amazonaws.com",
            "glue.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "pipeline_runner" {
  name = "${local.name_prefix}-pipeline-runner-policy"
  role = aws_iam_role.pipeline_runner.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ZoneReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          for b in aws_s3_bucket.zones : b.arn
        ]
      },
      {
        Sid    = "S3ObjectReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          for b in aws_s3_bucket.zones : "${b.arn}/*"
        ]
      },
      {
        Sid    = "PublishDataContractAlerts"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = aws_sns_topic.data_contract_alerts.arn
      },
      {
        Sid    = "KMSUsage"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.platform.arn
      }
    ]
  })
}
