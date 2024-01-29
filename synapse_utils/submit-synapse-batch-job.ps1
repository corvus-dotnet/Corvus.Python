[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]
    $AadTenantId,

    [Parameter(Mandatory=$true)]
    [string]
    $SubscriptionId,

    [Parameter(Mandatory=$true)]
    [string]
    $WorkspaceName,

    [Parameter(Mandatory=$true)]
    [string]
    $SparkPoolName,

    [Parameter(Mandatory=$true)]
    [string]
    $StorageAccountName,

    [Parameter(Mandatory=$true)]
    [string]
    $FileSystemName,

    [Parameter(Mandatory=$true)]
    [string]
    $PackageName,

    [Parameter(Mandatory=$true)]
    [string]
    $EntrypointFilePath
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $PSCommandPath

$corvusModuleInstalled = Get-Module -Name Corvus.Deployment -ListAvailable

if (!$corvusModuleInstalled) {
    Write-Host "Installing Corvus.Deployment module."
    Install-Module -Name Corvus.Deployment -Scope CurrentUser -Force
}

Import-Module Corvus.Deployment -Verbose:$false

$storageModule = Assert-CorvusModule -Name "Az.Storage" -Version 4.8.0
$synapseModule = Assert-CorvusModule -Name "Az.Synapse" -Version 1.5.0

$storageModule | Import-Module -Verbose:$false
$synapseModule | Import-Module -Verbose:$false


if(!(Get-AzContext)) {
    Write-Host "Signing into Azure."
    Connect-AzAccount -UseDeviceAuthentication -TenantId $AadTenantId -SubscriptionId $SubscriptionId
}

Invoke-Command -ScriptBlock { poetry build --format wheel }

$storageContext = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

# Uploading whl file
$localPackagePath = (Resolve-Path(Join-Path $here "../dist/$PackageName-0.0.1-py3-none-any.whl")).Path
$packageDestinationRelativePath = "/local-artifacts/packages/$PackageName-0.0.1-py3-none-any.whl"
Set-AzStorageBlobContent -Container $FileSystemName -File $localPackagePath -Blob $packageDestinationRelativePath -Context $storageContext -Force
$packageFilePath = "abfss://$FileSystemName@$StorageAccountName.dfs.core.windows.net/$packageDestinationRelativePath"


# Uploading entry point file
$entrypointFileName = Split-Path -Path $EntrypointFilePath -Leaf
$entrypointDestinationRelativePath = "/local-artifacts/entrypoints/$entrypointFileName"
Set-AzStorageBlobContent -Container $FileSystemName -File $EntrypointFilePath -Blob $entrypointDestinationRelativePath -Context $storageContext -Force

$sparkJobName = "localBatchJob"
$mainDefinitionFilePath = "abfss://$FileSystemName@$StorageAccountName.dfs.core.windows.net/$entrypointDestinationRelativePath"
$config = @{
    "spark.submit.pyFiles" = $packageFilePath
}

$job = Submit-AzSynapseSparkJob `
            -WorkspaceName $WorkspaceName `
            -SparkPoolName $SparkPoolName `
            -Language "PySpark" `
            -Name $sparkJobName `
            -MainDefinitionFile $mainDefinitionFilePath `
            -ExecutorCount "1" `
            -ExecutorSize "Small" `
            -Configuration $config

$workspaceResourceId = (Get-AzSynapseWorkspace -Name $WorkspaceName).Id

$workspaceResourceIdEncoded = [System.Web.HttpUtility]::UrlEncode($workspaceResourceId)

$monitoringUrl = "https://web.azuresynapse.net/en/monitoring/sparkapplication/$($sparkJobName)?workspace=$($workspaceResourceIdEncoded)&livyId=$($job.Id)&sparkPoolName=$($SparkPoolName)" 

Write-Host "`nBatch job in Synapse triggered successfully. Monitor at the following location: `n`n$monitoringUrl"
