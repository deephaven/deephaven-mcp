
variable "workspace" {
    description = "Terraform workspace"
}

data "assert_test" "workspace" {
    test = terraform.workspace == var.workspace
    throw = "Terraform workspace does not match tfvars workspace"
}
