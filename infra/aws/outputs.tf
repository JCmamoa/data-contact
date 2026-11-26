output "s3_buckets" {
  description = "S3 bucket names for raw, quarantine, curated, and Athena query result zones."
  value       = { for k, b in aws_s3_bucket.zones : k => b.bucket }
}

output "glue_databases" {
  description = "AWS Glue Data Catalog databases mirroring the medallion & semantic layers."
  value       = { for k, db in aws_glue_catalog_database.medallion : k => db.name }
}

output "athena_workgroup_name" {
  description = "Athena workgroup configured for semantic query execution."
  value       = aws_athena_workgroup.semantic_analytics.name
}

output "redshift_serverless_workgroup" {
  description = "Redshift Serverless workgroup name."
  value       = aws_redshiftserverless_workgroup.analytics_dw.workgroup_name
}

output "data_contract_alerts_sns_arn" {
  description = "SNS topic ARN for shift-left Data Contract CRITICAL violation alerts."
  value       = aws_sns_topic.data_contract_alerts.arn
}

output "pipeline_runner_role_arn" {
  description = "IAM role ARN assumed by ingestion & contract enforcement tasks."
  value       = aws_iam_role.pipeline_runner.arn
}
