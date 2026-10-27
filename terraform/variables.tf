variable "aws_region" {
  type        = string
  description = "Región de AWS para el despliegue de la infraestructura"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Ambiente de despliegue (dev, staging, prod)"
  default     = "dev"
}

variable "project_name" {
  type        = string
  description = "Nombre identificador del proyecto"
  default     = "mercado-nube"
}
