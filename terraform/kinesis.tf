resource "aws_kinesis_stream" "landing_stream" {
  name             = "jobsity-de-challenge-kinesis-stream"
  shard_count      = 1
  retention_period = 48

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}