$zerofailedExtensions = @(
    @{
        # References the extension from its GitHub repository. If not already installed, use latest version from 'main' will be downloaded.
        Name = "ZeroFailed.Build.Python"
        GitRepository = "https://github.com/zerofailed/ZeroFailed.Build.Python"
    }
)
. ZeroFailed.tasks -ZfPath $here/.zf

#
# Build process configuration
#
#
# Build process control options
#
$SkipInit = $false
$SkipVersion = $false
$SkipBuild = $false
$SkipTest = $false
$SkipRunPyTest = $false
$SkipTestReport = $false
$SkipAnalysis = $true
$SkipPackage = $false

# Python build config
$PythonProjectManager = 'poetry'
$PythonProjectDir = $here
$PythonSourceDirectory = 'src'
# $PythonPackageRepositoryUrl = "https://pkgs.dev.azure.com/ioccpvs/EDAP/_packaging/EDAP_feed/pypi/upload/"
$UseAzCliAuthForAzureArtifacts = $true

# Customise the build process
task . FullBuild

#
# Build Process Extensibility Points - uncomment and implement as required
#

# task RunFirst {}
# task PreInit {}
# task PostInit {}
# task PreVersion {}
# task PostVersion {}
# task PreBuild {}
# task PostBuild {}
# task PreTest {}
# task PostTest {}
# task PreTestReport {}
# task PostTestReport {}
# task PreAnalysis {}
# task PostAnalysis {}
# task PrePackage {}
# task PostPackage {}
# task PrePublish {}
# task PostPublish {}
# task RunLast {}