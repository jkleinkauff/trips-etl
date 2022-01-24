terraform {
  backend "s3" {
    bucket  = "xxx-de-challenge-terraform"
    key     = "challenge/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
    profile = "kleinkauffaws"
  }
}