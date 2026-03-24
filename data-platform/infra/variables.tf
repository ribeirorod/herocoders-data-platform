variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "eu-central-1"
}

variable "project" {
  description = "Project identifier used for resource naming and tagging"
  type        = string
  default     = "herocoders-data-platform"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}
