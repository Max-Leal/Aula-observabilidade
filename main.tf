terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_security_group" "sgDoMax" {
  name        = var.security_group_name
  description = "Security group para observabilidade"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = [22, 3300]

    content {
      description = "Allow port ${ingress.value}"
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = var.security_group_name
  }
}


resource "aws_instance" "instance-max" {

  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.sgDoMax.id]
  associate_public_ip_address = true
  user_data = base64encode(<<-EOF
              #!/bin/bash
              apt update -y
              apt install -y git docker.io docker-compose openssh-server
              systemctl enable --now ssh
              git clone https://github.com/Max-Leal/Aula-observabilidade.git
              cd ./Aula-observabilidade
              docker-compose up -d
              EOF
  )

  tags = {
    Name = "ec2-observability-max"
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "security_group_name" {
  type    = string
  default = "observability-max"
}

variable "vpc_id" {
  type    = string
  default = "vpc-06786ee7f7a163059"
}

variable "subnet_id" {
  type    = string
  default = "subnet-08ab1cc11d069cf59"
}

variable "instance_type" {
  type    = string
  default = "t3.nano"
}

variable "ami_id" {
  type    = string
  default = "ami-0f9c27b471bdcd702" // debian
}