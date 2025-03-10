data "aws_iam_policy_document" "snowflake_trust_policy" {
  statement {
    sid     = "AllowSnowflakeViaExternalID"
    actions = ["sts:AssumeRole"]
    effect  = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::539247493663:user/xc6x0000-s"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["TV69155_SFCRole=3_MMcjEEgncJ5fVahR1Yi7eiMn6YM="]
    }
  }
}

resource "aws_iam_role" "snowflake_role" {
  name               = "SnowflakeS3Role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust_policy.json
}


data "aws_iam_policy_document" "snowflake_s3_policy_doc" {
  statement {
    sid     = "GetObjects"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::${aws_s3_bucket.data_landing_zone.bucket}/*",
      "arn:aws:s3:::${aws_s3_bucket.data_landing_zone.bucket}"
      ]
  }
}

resource "aws_iam_policy" "snowflake_s3_policy" {
  name   = "SnowflakeS3Policy"
  policy = data.aws_iam_policy_document.snowflake_s3_policy_doc.json
}
resource "aws_iam_role_policy_attachment" "snowflake_role_attachment" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}


resource "snowflake_storage_integration" "s3_int" {
  name                = "S3_INT"
  storage_provider    = "S3"
  enabled             = true

  storage_aws_role_arn = aws_iam_role.snowflake_role.arn

  storage_allowed_locations = [
    "s3://${aws_s3_bucket.data_landing_zone.bucket}/"
  ]

  comment = "Snowflake Integration using user-defined external ID"
}
