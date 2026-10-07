# Provision the Azure resources for the AML lakehouse: resource group, ADLS Gen2 storage, Databricks workspace,
# and a Databricks access connector (managed identity) that Unity Catalog uses to reach the storage.
# Prereq: az login; az account set --subscription <id>.  Cost: Databricks bills per cluster hour, storage per GB. Tear down with 99_teardown.ps1.
# Run as a file, not pasted into a terminal:  powershell -ExecutionPolicy Bypass -File .\infra\azure\01_provision.ps1 -Location northeurope
param(
    [string]$Location = "uaenorth",
    [string]$Prefix = "amllake",
    [string]$Suffix = ""
)
$ErrorActionPreference = "Stop"
if (-not $Suffix) { $Suffix = -join ((97..122) + (48..57) | Get-Random -Count 5 | ForEach-Object { [char]$_ }) }
$rg = "rg-$Prefix-dev"
$storage = "st$Prefix$Suffix"            # 3-24 chars, lowercase letters and digits, globally unique
$workspace = "dbw-$Prefix-dev"
$connector = "dbac-$Prefix-dev"

# az is a native command, so $ErrorActionPreference does not catch its failures; check the exit code after each call.
function Invoke-Az {
    $out = & az @args
    if ($LASTEXITCODE -ne 0) { throw "az $($args -join ' ') failed (exit $LASTEXITCODE)" }
    return $out
}

Invoke-Az extension add --name databricks --only-show-errors | Out-Null
Invoke-Az group create --name $rg --location $Location --tags project=aml-monitoring-lakehouse | Out-Null

Invoke-Az storage account create --name $storage --resource-group $rg --location $Location `
    --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true --min-tls-version TLS1_2 `
    --allow-blob-public-access false | Out-Null
foreach ($c in "landing", "lake") {
    Invoke-Az storage container create --account-name $storage --name $c --auth-mode login | Out-Null
}

Invoke-Az databricks workspace create --name $workspace --resource-group $rg --location $Location --sku premium | Out-Null
Invoke-Az databricks access-connector create --name $connector --resource-group $rg --location $Location `
    --identity-type SystemAssigned | Out-Null

$principal = Invoke-Az databricks access-connector show --name $connector --resource-group $rg --query identity.principalId -o tsv
$storageId = Invoke-Az storage account show --name $storage --resource-group $rg --query id -o tsv
Invoke-Az role assignment create --assignee-object-id $principal --assignee-principal-type ServicePrincipal `
    --role "Storage Blob Data Contributor" --scope $storageId | Out-Null

$url = Invoke-Az databricks workspace show --name $workspace --resource-group $rg --query workspaceUrl -o tsv
$connectorId = Invoke-Az databricks access-connector show --name $connector --resource-group $rg --query id -o tsv
@{ resource_group = $rg; storage_account = $storage; workspace = $workspace; workspace_url = "https://$url"; access_connector_id = $connectorId } |
    ConvertTo-Json | Set-Content -Encoding utf8 "$PSScriptRoot/azure_outputs.json"
Write-Host "Done. Outputs saved to infra/azure/azure_outputs.json"
