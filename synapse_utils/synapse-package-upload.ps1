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
    $FullPackageName
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $PSCommandPath

$corvusModuleInstalled = Get-Module -Name Corvus.Deployment -ListAvailable

if (!$corvusModuleInstalled) {
    Write-Host "Installing Corvus.Deployment module."
    Install-Module -Name Corvus.Deployment -Scope CurrentUser -Force
}

Import-Module Corvus.Deployment -Verbose:$false

$synapseModule = Assert-CorvusModule -Name "Az.Synapse" -Version 1.5.0

$synapseModule | Import-Module -Verbose:$false

if(!(Get-AzContext)) {
    Write-Host "Signing into Azure."
    Connect-CorvusAzure -AadTenantId $AadTenantId -SubscriptionId $SubscriptionId -SkipAzureCli 
}

$packagePath = (Resolve-Path(Join-Path $here "../dist/$FullPackageName")).Path

# Upload whl package to Synapse workspace
$workspacePackage = New-AzSynapseWorkspacePackage -WorkspaceName $WorkspaceName -Package $packagePath

# Assign new package to Spark Pool, removing any existing packages from pool before uploading (to avoid conflicts)
$pool = Get-AzSynapseSparkPool -WorkspaceName $WorkspaceName -Name $SparkPoolName

if ($pool.WorkspacePackages) {
    Write-Host "Removing existing package from pool."
    $pool | Update-AzSynapseSparkPool -PackageAction Remove -Package $pool.WorkspacePackages
}

Write-Host "Uploading new package to pool. This generally takes 10-15 minutes."
Update-AzSynapseSparkPool -WorkspaceName $WorkspaceName -Name $SparkPoolName -PackageAction Add -Package $workspacePackage
