resource "null_resource" "deploy_airflow" {
  triggers = {
    compose_file_checksum = sha1(file("../airflow/docker-compose.yml"))
  }

  provisioner "local-exec" {
    environment = {
      AWS_ACCESS_KEY_ID     = var.aws_access_key_id
      AWS_SECRET_ACCESS_KEY = var.aws_secret_access_key
      AWS_REGION            = var.aws_region
      AWS_LANDING_BUCKET_NAME = aws_s3_bucket.data_landing_zone.id
      AIRFLOW_CONN_SNOWFLAKE_DEFAULT = "snowflake://${var.snowflake_username}:${var.snowflake_password}@${var.snowflake_account_id}/${snowflake_database.this.name}?warehouse=${snowflake_warehouse.this.name}&role=${var.snowflake_role}"
      SNOWFLAKE_ACCOUNT = var.snowflake_account_id
      SNOWFLAKE_USER = var.snowflake_username
      SNOWFLAKE_PASSWORD = var.snowflake_password
      SNOWFLAKE_DATABASE = snowflake_database.this.name
      SNOWFLAKE_WH = snowflake_warehouse.this.name
      SNOWFLAKE_ROLE = var.snowflake_role
    }
    command = "docker compose -f ../airflow/docker-compose.yml up -d"
  }
}
