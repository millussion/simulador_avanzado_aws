output "s3_data_lake_bucket_name" {
  description = "Nombre del bucket S3 principal del Data Lake"
  value       = aws_s3_bucket.data_lake.id
}

output "execution_role_arn" {
  description = "ARN del rol IAM de ejecución con permisos mínimos"
  value       = aws_iam_role.pipeline_execution_role.arn
}
