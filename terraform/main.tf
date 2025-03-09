terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Optionally include snowflake provider here
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.40" # example version
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 2.16.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_username
  password          = var.snowflake_password

  warehouse = var.snowflake_warehouse
  role     = "ACCOUNTADMIN"
}

provider "docker" {
  host = "npipe:////./pipe/docker_engine"
}