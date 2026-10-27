# Rol de Ejecución de Tareas (Airflow / Worker / ECS)
resource "aws_iam_role" "pipeline_execution_role" {
  name = "${var.project_name}-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "ecs-tasks.amazonaws.com",
            "airflow.amazonaws.com"
          ]
        }
      }
    ]
  })
}

# Política con permisos mínimos exclusivos sobre el Data Lake y CloudWatch Logs
resource "aws_iam_policy" "pipeline_execution_policy" {
  name        = "${var.project_name}-execution-policy-${var.environment}"
  description = "Permisos de lectura y escritura acotados al bucket del Data Lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DataLakeS3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_execution_policy" {
  role       = aws_iam_role.pipeline_execution_role.name
  policy_arn = aws_iam_policy.pipeline_execution_policy.arn
}
