# BCP Import Script for Windows PowerShell
# This script imports data from Azure Blob Storage into SQL Server using BCP

param(
    [string]$ConfigFile = "..\config\azure_config.json",
    [switch]$UseLocalFile = $false,
    [switch]$Verbose = $false
)

# Function to log messages
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Output $logMessage
    Add-Content -Path "bcp_import.log" -Value $logMessage
}

# Function to check prerequisites
function Test-Prerequisites {
    Write-Log "Checking prerequisites..."
    
    # Check if BCP is available
    try {
        $bcpVersion = & bcp -v 2>&1
        Write-Log "BCP utility found: $bcpVersion"
    }
    catch {
        Write-Log "ERROR: BCP utility not found. Please install SQL Server command-line tools." "ERROR"
        Write-Output "Download from: https://docs.microsoft.com/en-us/sql/tools/bcp-utility"
        exit 1
    }
    
    # Check if config file exists
    if (-not (Test-Path $ConfigFile)) {
        Write-Log "ERROR: Configuration file not found: $ConfigFile" "ERROR"
        exit 1
    }
}

# Function to load configuration
function Get-Configuration {
    param([string]$ConfigPath)
    
    Write-Log "Loading configuration from: $ConfigPath"
    
    try {
        $config = Get-Content $ConfigPath | ConvertFrom-Json
        Write-Log "Configuration loaded successfully"
        return $config
    }
    catch {
        Write-Log "ERROR: Failed to load configuration: $($_.Exception.Message)" "ERROR"
        exit 1
    }
}

# Function to get latest blob file
function Get-LatestBlobFile {
    param($Config)
    
    Write-Log "Finding latest data file in blob storage..."
    
    $storageAccount = $config.storage_account.name
    $container = $config.storage_account.container
    $sasToken = $config.storage_account.sas_token
    
    # In production, you would use Azure PowerShell to list blobs
    # For this example, we'll construct the expected path
    $blobPattern = "sales_data_aggregated_*"
    $blobUrl = "https://$storageAccount.blob.core.windows.net/$container/$blobPattern$sasToken"
    
    Write-Log "Blob URL pattern: $blobUrl"
    return $blobUrl
}

# Function to download blob file
function Download-BlobFile {
    param([string]$BlobUrl)
    
    $localFile = "temp_data_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
    Write-Log "Downloading blob file to: $localFile"
    
    try {
        Invoke-WebRequest -Uri $BlobUrl -OutFile $localFile -UseBasicParsing
        Write-Log "File downloaded successfully"
        return $localFile
    }
    catch {
        Write-Log "ERROR: Failed to download blob file: $($_.Exception.Message)" "ERROR"
        return $null
    }
}

# Function to run BCP import
function Invoke-BCPImport {
    param(
        [string]$DataSource,
        [object]$Config,
        [bool]$IsUrl = $false
    )
    
    Write-Log "Starting BCP import..."
    Write-Log "Data source: $DataSource"
    
    $sqlServer = $Config.sql_server.server
    $database = $Config.sql_server.database
    $username = $Config.sql_server.username
    $password = $Config.sql_server.password
    $table = $Config.sql_server.table
    $batchSize = $Config.pipeline.batch_size
    
    $targetTable = "$database.dbo.$table"
    $errorLog = "bcp_import_errors_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
    $formatFile = "..\sql\format_file.fmt"
    
    Write-Log "Target: $sqlServer.$targetTable"
    
    # Build BCP arguments
    $bcpArgs = @(
        $targetTable,
        "in",
        $DataSource,
        "-S", $sqlServer,
        "-d", $database,
        "-U", $username,
        "-P", $password,
        "-c",                    # Character data type
        "-t", "|",              # Field terminator
        "-r", "`n",             # Row terminator
        "-b", $batchSize,       # Batch size
        "-m", "0",              # Max errors
        "-e", $errorLog,        # Error log file
        "-C", "65001"           # UTF-8 encoding
    )
    
    # Add format file if it exists
    if (Test-Path $formatFile) {
        $bcpArgs += @("-f", $formatFile)
        Write-Log "Using format file: $formatFile"
    }
    
    if ($Verbose) {
        Write-Log "BCP Command: bcp $($bcpArgs -join ' ')"
    }
    
    try {
        Write-Log "Executing BCP import..."
        $result = & bcp @bcpArgs
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "BCP import completed successfully"
            Write-Log "Result: $result"
            
            # Check for errors
            if (Test-Path $errorLog) {
                $errorContent = Get-Content $errorLog
                if ($errorContent) {
                    Write-Log "WARNING: Errors occurred during import:" "WARN"
                    Write-Log $errorContent "WARN"
                }
            }
            
            return $true
        }
        else {
            Write-Log "ERROR: BCP import failed with exit code: $LASTEXITCODE" "ERROR"
            if (Test-Path $errorLog) {
                $errorContent = Get-Content $errorLog
                Write-Log "Error details: $errorContent" "ERROR"
            }
            return $false
        }
    }
    catch {
        Write-Log "ERROR: Exception during BCP import: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to verify import
function Test-ImportSuccess {
    param($Config)
    
    Write-Log "Verifying import..."
    
    $sqlServer = $Config.sql_server.server
    $database = $Config.sql_server.database
    $username = $Config.sql_server.username
    $password = $Config.sql_server.password
    $table = $Config.sql_server.table
    
    $verifyQuery = "SELECT COUNT(*) as record_count, MIN(sale_date) as min_date, MAX(sale_date) as max_date FROM dbo.$table"
    
    try {
        # Use sqlcmd if available
        $sqlcmdArgs = @(
            "-S", $sqlServer,
            "-d", $database,
            "-U", $username,
            "-P", $password,
            "-Q", $verifyQuery,
            "-h", "-1",
            "-s", ","
        )
        
        $result = & sqlcmd @sqlcmdArgs 2>&1
        Write-Log "Verification query result: $result"
    }
    catch {
        Write-Log "sqlcmd not available or failed. Please verify import manually." "WARN"
    }
}

# Function to cleanup temporary files
function Remove-TempFiles {
    Write-Log "Cleaning up temporary files..."
    Get-ChildItem -Path "." -Filter "temp_data_*.csv" | Remove-Item -Force
    Write-Log "Cleanup completed"
}

# Main execution
function Main {
    Write-Log "=== BCP Import Process Started ==="
    
    try {
        # Check prerequisites
        Test-Prerequisites
        
        # Load configuration
        $config = Get-Configuration -ConfigPath $ConfigFile
        
        # Get blob file
        $blobUrl = Get-LatestBlobFile -Config $config
        
        $importSuccess = $false
        
        if ($UseLocalFile) {
            Write-Log "Using local file approach..."
            $localFile = Download-BlobFile -BlobUrl $blobUrl
            if ($localFile) {
                $importSuccess = Invoke-BCPImport -DataSource $localFile -Config $config -IsUrl $false
            }
        }
        else {
            Write-Log "Attempting direct import from blob URL..."
            $importSuccess = Invoke-BCPImport -DataSource $blobUrl -Config $config -IsUrl $true
            
            if (-not $importSuccess) {
                Write-Log "Direct import failed, trying local file approach..."
                $localFile = Download-BlobFile -BlobUrl $blobUrl
                if ($localFile) {
                    $importSuccess = Invoke-BCPImport -DataSource $localFile -Config $config -IsUrl $false
                }
            }
        }
        
        if ($importSuccess) {
            Write-Log "Import process completed successfully"
            Test-ImportSuccess -Config $config
        }
        else {
            Write-Log "Import process failed" "ERROR"
            exit 1
        }
    }
    catch {
        Write-Log "ERROR: Unexpected error: $($_.Exception.Message)" "ERROR"
        exit 1
    }
    finally {
        Remove-TempFiles
        Write-Log "=== BCP Import Process Completed ==="
    }
}

# Run main function
Main
