variable "project_name" {
  type        = string
  description = "Name prefix for all resources"
  default     = "RETAIL360"
}

variable "aws_region" {
  type        = string
  description = "AWS region where resources will be created"
  default     = "us-east-2"
}

variable "environment" {
  type        = string
  description = "Environment name (dev, staging, prod)"
  default     = "DEV"
}

# Airflow-related variables
variable "airflow_name" {
  type        = string
  description = "Name for the Airflow environment"
  default     = "airflow-retail360-dev"
}

variable "airflow_version" {
  type        = string
  description = "Apache Airflow version"
  default     = "2.10.3"
}


# Snowflake variables
variable "snowflake_account_name" {
  type        = string
  description = "Snowflake account name"
}

variable "snowflake_account_id" {
  type        = string
  description = "Snowflake account id"
}

variable "snowflake_organization_name" {
  type        = string
  description = "Snowflake Organization Name"
}

variable "snowflake_username" {
  type        = string
  description = "Snowflake username"
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password"
}

variable "snowflake_region" {
  type        = string
  description = "Snowflake region"
  default     = "aws_us_east_2"
}

variable "snowflake_warehouse" {
  type        = string
  description = "Snowflake Default Warehouse"
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake Role"
}


variable "aws_access_key_id" {
  type        = string
  description = "AWS Access Key ID for the local Airflow container"
}

variable "aws_secret_access_key" {
  type        = string
  description = "AWS Secret Access Key for the local Airflow container"
  sensitive   = true
}

