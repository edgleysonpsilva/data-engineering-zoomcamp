```bash
terraform init
terraform apply -auto-approve
terraform destroy
``` 
Terraform init is the first because that's the standard first step to download plugins and configure the backend. 
Terraform apply with the -auto-approve flag or otherwise the process would pause and wait for user to type "yes" manually. 
Terraform destroy to clear out all the managed resources as requested.