resource "aws_kinesis_stream" "kinesis" {
  name = ""
  shard_count = 0
}

resource "aws_lambda_function" "lambda" {
  function_name = ""
  handler = ""
  role = ""
  runtime = ""
}

resource "aws_s3_bucket" "s3" {
  bucket = ""
}

resource "aws_elasticache_cluster" "elasticache" {
  cluster_id = ""
}