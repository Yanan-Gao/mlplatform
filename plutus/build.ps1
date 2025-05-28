param (
    [switch]$commit
)

# Get the current Git branch name
$branchName = git rev-parse --abbrev-ref HEAD

# Get the latest Git commit SHA
$commitSha = git rev-parse --short HEAD

# Define variables
$plutusJarName = "plutus.jar"
$ciProjectDir = "."  # Replace with your actual project path
$assemblyOut = "$ciProjectDir\target\scala-2.12\$plutusJarName"
$baseS3Folder = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/mergerequests/$branchName"


# Define destination paths with branch name
$dest = "$baseS3Folder/$commitSha"
$destLatest = "$baseS3Folder/latest"

# Run sbt build and AWS upload based on --commit flag
if ($commit) {
    Write-Host "Running sbt build..."
    sbt 'set test in assembly := {}' clean assembly

    Write-Host "Uploading to S3..."
    aws s3 cp $assemblyOut "$dest/$plutusJarName"
    aws s3 cp $assemblyOut "$destLatest/$plutusJarName"
} else {
    Write-Host "Simulating sbt build..."
    Write-Host "Simulating: sbt 'set test in assembly := {}' clean assembly"

    Write-Host "Simulating AWS S3 uploads..."
    Write-Host "Simulating: aws s3 cp $assemblyOut $dest/$plutusJarName"
    Write-Host "Simulating: aws s3 cp $assemblyOut $destLatest/$plutusJarName"
}
