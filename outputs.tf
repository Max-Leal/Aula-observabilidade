output "public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = "Observability at: http://${aws_instance.instance-max.public_ip}:3300"
}
