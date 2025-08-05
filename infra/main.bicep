targetScope = 'resourceGroup'

@description('The base name for all resources. A random suffix will be added.')
param baseName string = 'sparkbcp'

@description('The location for all resources')
param location string = resourceGroup().location

@description('Environment name (e.g., dev, test, prod)')
param environment string = 'dev'

@description('SQL Server administrator username')
param sqlAdminUsername string = 'sqladmin'

@description('SQL Server administrator password')
@secure()
param sqlAdminPassword string

@description('Storage account SKU')
@allowed(['Standard_LRS', 'Standard_GRS', 'Standard_ZRS', 'Premium_LRS'])
param storageAccountSku string = 'Standard_LRS'

@description('SQL Database SKU name')
param sqlDatabaseSkuName string = 'Basic'

@description('SQL Database tier')
param sqlDatabaseTier string = 'Basic'

@description('SQL Database capacity (DTUs or vCores)')
param sqlDatabaseCapacity int = 5

@description('Allow access from Azure services')
param allowAzureServices bool = true

@description('Your IP address for SQL Server firewall (optional)')
param clientIPAddress string = ''

@description('Tags to apply to all resources')
param tags object = {
  Environment: environment
  Project: 'Spark-BCP-Pipeline'
  'Created-By': 'Bicep'
}

// Generate a unique suffix for resource names
var resourceToken = toLower(uniqueString(subscription().id, resourceGroup().id))
var storageAccountName = '${baseName}stg${resourceToken}'
var sqlServerName = '${baseName}-sql-${resourceToken}'
var sqlDatabaseName = '${baseName}-db'
var containerName = 'data-pipeline'

// Storage Account
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: union(tags, {
    ResourceType: 'Storage'
  })
  sku: {
    name: storageAccountSku
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
    encryption: {
      services: {
        blob: {
          enabled: true
          keyType: 'Account'
        }
        file: {
          enabled: true
          keyType: 'Account'
        }
      }
      keySource: 'Microsoft.Storage'
      requireInfrastructureEncryption: false
    }
  }
}

// Blob Service
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

// Container for pipeline data
resource dataContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
    metadata: {
      purpose: 'spark-bcp-pipeline'
      environment: environment
    }
  }
}

// SQL Server
resource sqlServer 'Microsoft.Sql/servers@2021-11-01' = {
  name: sqlServerName
  location: location
  tags: union(tags, {
    ResourceType: 'SQL Server'
  })
  properties: {
    administratorLogin: sqlAdminUsername
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

// SQL Database
resource sqlDatabase 'Microsoft.Sql/servers/databases@2021-11-01' = {
  parent: sqlServer
  name: sqlDatabaseName
  location: location
  tags: union(tags, {
    ResourceType: 'SQL Database'
  })
  sku: {
    name: sqlDatabaseSkuName
    tier: sqlDatabaseTier
    capacity: sqlDatabaseCapacity
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 2147483648 // 2GB for Basic tier
    catalogCollation: 'SQL_Latin1_General_CP1_CI_AS'
    zoneRedundant: false
    readScale: 'Disabled'
    requestedBackupStorageRedundancy: 'Geo'
  }
}

// Firewall rule to allow Azure services
resource sqlFirewallRuleAzure 'Microsoft.Sql/servers/firewallRules@2021-11-01' = if (allowAzureServices) {
  parent: sqlServer
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// Firewall rule for client IP (if provided)
resource sqlFirewallRuleClient 'Microsoft.Sql/servers/firewallRules@2021-11-01' = if (!empty(clientIPAddress)) {
  parent: sqlServer
  name: 'ClientIPAccess'
  properties: {
    startIpAddress: clientIPAddress
    endIpAddress: clientIPAddress
  }
}

// Outputs
output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output storageAccountPrimaryKey string = storageAccount.listKeys().keys[0].value
output storageConnectionString string = 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccount.listKeys().keys[0].value};EndpointSuffix=${az.environment().suffixes.storage}'
output containerName string = containerName
output blobEndpoint string = storageAccount.properties.primaryEndpoints.blob

output sqlServerName string = sqlServer.name
output sqlServerId string = sqlServer.id
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName string = sqlDatabase.name
output sqlDatabaseId string = sqlDatabase.id

output resourceGroupName string = resourceGroup().name
output location string = location
output environment string = environment
