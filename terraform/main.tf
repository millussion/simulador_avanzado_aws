terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-data-lake-${var.environment}"
  force_destroy = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Bloqueo de acceso público (Seguridad)
resource "aws_s3_bucket_public_access_block" "data_lake_pab" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Cifrado en reposo (SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Políticas de ciclo de vida (Transición a Glacier y borrado de logs/cuarentena antigua)
resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "archive-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# Estructura de prefijos para capas Medallion
resource "aws_s3_object" "lake_layers" {
  for_each = toset(["raw/", "cleaned/", "curated/", "quarantine/"])
  bucket   = aws_s3_bucket.data_lake.id
  key      = each.value
}
