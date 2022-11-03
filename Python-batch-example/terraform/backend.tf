terraform {
  backend "s3" {
    bucket = ""
    workspace_key_prefix = ""
    key = ""
    region = ""
    encrypt = true
  }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}