# Delete everything created by 01_provision.ps1 (the whole resource group). Irreversible.
param([string]$ResourceGroup = "rg-amllake-dev")
az group delete --name $ResourceGroup --yes --no-wait
Write-Host "Deleting $ResourceGroup in the background."
