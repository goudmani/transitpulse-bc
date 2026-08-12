output "vpc_id" {
  value = aws_vpc.main.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "glue_security_group_id" {
  value = aws_security_group.glue.id
}
