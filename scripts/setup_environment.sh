#!/bin/bash

# Environment Setup Script
# This script sets up the development environment for the Spark to BCP pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up Spark to Azure Blob to BCP Pipeline environment..."
echo "Project root: $PROJECT_ROOT"

# Function to check if running on supported OS
check_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "✅ Detected Linux environment"
        DISTRO=$(lsb_release -si 2>/dev/null || echo "Unknown")
        echo "Distribution: $DISTRO"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "✅ Detected macOS environment"
    else
        echo "⚠️  Warning: Untested OS environment: $OSTYPE"
    fi
}

# Function to install system dependencies
install_system_deps() {
    echo "Installing system dependencies..."
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Update package list
        sudo apt-get update
        
        # Install essential packages
        sudo apt-get install -y \
            python3 \
            python3-pip \
            python3-venv \
            curl \
            wget \
            jq \
            unzip \
            openjdk-11-jdk
        
        # Install Microsoft ODBC driver and tools
        echo "Installing Microsoft SQL Server tools..."
        
        # Add Microsoft repository
        curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
        
        # Get Ubuntu version
        UBUNTU_VERSION=$(lsb_release -rs)
        curl https://packages.microsoft.com/config/ubuntu/${UBUNTU_VERSION}/prod.list | \
            sudo tee /etc/apt/sources.list.d/msprod.list
        
        sudo apt-get update
        
        # Install SQL Server tools (accept EULA automatically)
        ACCEPT_EULA=Y sudo apt-get install -y mssql-tools18 unixodbc-dev
        
        # Add to PATH
        echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
        
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS using Homebrew
        if ! command -v brew &> /dev/null; then
            echo "Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        
        brew install python3 jq openjdk@11
        
        # Install Microsoft SQL Server tools
        brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
        brew install mssql-tools18
    fi
    
    echo "✅ System dependencies installed"
}

# Function to setup Python environment
setup_python_env() {
    echo "Setting up Python virtual environment..."
    
    cd "$PROJECT_ROOT"
    
    # Create virtual environment
    python3 -m venv venv
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install Python requirements
    if [ -f "requirements.txt" ]; then
        echo "Installing Python packages..."
        pip install -r requirements.txt
    else
        echo "⚠️  requirements.txt not found, installing basic packages..."
        pip install pyspark pandas azure-storage-blob azure-identity pyodbc
    fi
    
    echo "✅ Python environment setup complete"
}

# Function to setup Spark
setup_spark() {
    echo "Setting up Apache Spark..."
    
    SPARK_VERSION="3.5.0"
    HADOOP_VERSION="3"
    SPARK_DIR="$PROJECT_ROOT/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
    
    if [ ! -d "$SPARK_DIR" ]; then
        echo "Downloading Apache Spark ${SPARK_VERSION}..."
        
        cd "$PROJECT_ROOT"
        curl -O "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
        
        echo "Extracting Spark..."
        tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
        
        # Create symbolic link
        ln -sf "$SPARK_DIR" spark-current
    fi
    
    # Set environment variables
    cat >> ~/.bashrc << EOF

# Spark Environment Variables
export SPARK_HOME="$SPARK_DIR"
export PATH="\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin"
export PYSPARK_PYTHON=python3
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

EOF
    
    echo "✅ Spark setup complete"
    echo "Please run 'source ~/.bashrc' to load environment variables"
}

# Function to setup Azure CLI (optional)
setup_azure_cli() {
    echo "Setting up Azure CLI (optional)..."
    
    if ! command -v az &> /dev/null; then
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            brew install azure-cli
        fi
        
        echo "✅ Azure CLI installed"
        echo "Run 'az login' to authenticate with Azure"
    else
        echo "✅ Azure CLI already installed"
    fi
}

# Function to create directory structure
create_directories() {
    echo "Creating directory structure..."
    
    cd "$PROJECT_ROOT"
    
    mkdir -p logs
    mkdir -p temp
    mkdir -p output
    
    echo "✅ Directory structure created"
}

# Function to setup configuration template
setup_config_template() {
    echo "Setting up configuration template..."
    
    CONFIG_FILE="$PROJECT_ROOT/config/azure_config.json"
    
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "⚠️  Configuration file not found: $CONFIG_FILE"
        echo "Please update the configuration file with your Azure credentials and SQL Server details"
        echo ""
        echo "Required configuration:"
        echo "1. Azure Storage Account details"
        echo "2. SQL Server connection information"
        echo "3. Azure AD credentials (optional but recommended)"
    else
        echo "✅ Configuration file exists"
    fi
}

# Function to test installation
test_installation() {
    echo "Testing installation..."
    
    # Test Python environment
    source "$PROJECT_ROOT/venv/bin/activate"
    
    echo "Testing Python packages..."
    python3 -c "import pyspark; print('✅ PySpark:', pyspark.__version__)"
    python3 -c "import pandas; print('✅ Pandas:', pandas.__version__)"
    
    # Test Java
    if command -v java &> /dev/null; then
        echo "✅ Java: $(java -version 2>&1 | head -1)"
    else
        echo "❌ Java not found"
    fi
    
    # Test BCP
    if command -v bcp &> /dev/null; then
        echo "✅ BCP utility available"
    else
        echo "❌ BCP utility not found"
        echo "Please add /opt/mssql-tools18/bin to your PATH"
    fi
    
    # Test Azure CLI
    if command -v az &> /dev/null; then
        echo "✅ Azure CLI: $(az --version | head -1)"
    else
        echo "⚠️  Azure CLI not installed (optional)"
    fi
    
    echo "✅ Installation test complete"
}

# Function to display next steps
show_next_steps() {
    echo ""
    echo "============================================"
    echo "🎉 Environment setup complete!"
    echo "============================================"
    echo ""
    echo "Next steps:"
    echo "1. Update configuration file: config/azure_config.json"
    echo "2. Create target SQL Server table: sql/create_table.sql"
    echo "3. Test the pipeline: ./scripts/run_pipeline.sh"
    echo ""
    echo "To activate the Python environment:"
    echo "  source venv/bin/activate"
    echo ""
    echo "To run individual components:"
    echo "  ./scripts/run_pipeline.sh spark-only"
    echo "  ./scripts/run_pipeline.sh bcp-only"
    echo ""
    echo "For help:"
    echo "  ./scripts/run_pipeline.sh help"
    echo ""
}

# Main execution
main() {
    echo "=================================================="
    echo "Spark to Azure Blob to BCP Pipeline Setup"
    echo "=================================================="
    
    check_os
    install_system_deps
    setup_python_env
    setup_spark
    setup_azure_cli
    create_directories
    setup_config_template
    test_installation
    show_next_steps
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [help|system-only|python-only|spark-only]"
        echo ""
        echo "Options:"
        echo "  help         Show this help message"
        echo "  system-only  Install only system dependencies"
        echo "  python-only  Setup only Python environment"
        echo "  spark-only   Setup only Spark environment"
        echo "  (no args)    Full environment setup"
        exit 0
        ;;
    "system-only")
        check_os
        install_system_deps
        ;;
    "python-only")
        setup_python_env
        ;;
    "spark-only")
        setup_spark
        ;;
    *)
        main
        ;;
esac
