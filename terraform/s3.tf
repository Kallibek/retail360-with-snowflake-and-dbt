resource "random_id" "suffix" {
  keepers = {
    env = var.environment
  }
  byte_length = 4
}

provider "random" {}


# Bucket for storing the synthetic data files
resource "aws_s3_bucket" "data_landing_zone" {
  bucket = lower("${var.project_name}-data-${var.environment}-${random_id.suffix.hex}")

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "data_landing_zone_block" {
  bucket                  = aws_s3_bucket.data_landing_zone.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}