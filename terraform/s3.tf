resource "aws_s3_bucket" "s3-landing" {
  bucket = "xxx-de-challenge-landing"
  acl    = "private"

  versioning {
    enabled = true
  }

}

resource "aws_s3_bucket" "s3-staging" {
  bucket = "xxx-de-challenge-staging"
  acl    = "private"

  versioning {
    enabled = true
  }

}

resource "aws_s3_bucket" "s3-processed" {
  bucket = "xxx-de-challenge-processed"
  acl    = "private"

  versioning {
    enabled = true
  }

}
