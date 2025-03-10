resource "snowflake_warehouse" "this" {
  name           = "${var.project_name}_${var.environment}_WH"
  warehouse_size = "XSMALL" 
  auto_suspend   = 300  # seconds
  auto_resume    = true
  initially_suspended = true

  comment = "Warehouse for Retail360 project"
}

resource "snowflake_database" "this" {
  name    = "${var.project_name}_${var.environment}_DB"
  comment = "Database for Retail360 project"
}

resource "snowflake_schema" "staging" {
  name       = "STAGING"
  database   = snowflake_database.this.name
  comment    = "Staging schema for raw data"
}

resource "snowflake_schema" "core" {
  name       = "CORE"
  database   = snowflake_database.this.name
  comment    = "Core transformations (silver layer)"
}

resource "snowflake_schema" "marts" {
  name       = "MARTS"
  database   = snowflake_database.this.name
  comment    = "Data mart (gold layer) schema"
}

resource "snowflake_stage" "landing_stage" {
  name        = "LANDING_STAGE"
  database    = snowflake_database.this.name
  schema      = snowflake_schema.staging.name
  url         = "s3://${aws_s3_bucket.data_landing_zone.bucket}/"
  comment     = "Stage to load data from S3"
  credentials = "AWS_KEY_ID='${var.aws_access_key_id}' AWS_SECRET_KEY='${var.aws_secret_access_key}'"
  #storage_integration = snowflake_storage_integration.s3_int.name
}

resource "snowflake_file_format" "csv" {
  name        = "MY_CSV"
  database    = snowflake_database.this.name
  schema      = snowflake_schema.staging.name
  format_type = "CSV"
  skip_header = 1
  field_optionally_enclosed_by = "\""
}
