#!/bin/bash

# Install BCP (Bulk Copy Program) for SQL Server on Linux/Unix systems
# usage: ./install_bcp.sh

# Add Microsoft repository
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list

# Update package list
sudo apt-get update

# Install SQL Server tools (includes BCP)
sudo apt-get install mssql-tools18 unixodbc-dev

# Add to PATH
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc