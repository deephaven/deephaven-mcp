# Terraform: docker-repo

This Terraform directory creates a Docker repository in Google Cloud Registry.

**This should only need to be run once**
**Do not delete the repo unless you know what you are doing!  It contains Docker images for what has been spun up on the cloud.**

To authenticate:
```bash
gcloud auth application-default login
```

To init:
```bash
terraform init
```

To plan actions:
```bash
terraform plan
```

To apply actions:
```bash
terraform apply
```

To see the repo in the Google Cloud Console, navigate to:
* [Google Cloud Console Artifact Repository](https://console.cloud.google.com/artifacts)
