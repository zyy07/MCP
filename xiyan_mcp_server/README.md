
# XiYan MCP Server

A Model Context Protocol (MCP) server that enables secure interaction with MySQL databases. This server allows AI assistants to list tables, read data, and execute SQL queries through a controlled interface, making database exploration and analysis safer and more structured.

## Features
- Fetch data by natural language throught XiYanSQL (https://github.com/XGenerationLab/XiYan-SQL)
- List available MySQL tables as resources
- Read table contents

## Installation

```bash
pip install xiyan-mcp-server
```

## Configuration

Set the following environment variables:

```bash
MYSQL_HOST=    # Database host
MYSQL_PORT=         # Optional: Database port (defaults to 3306 if not specified)
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DATABASE=
MODEL_NAME=       
MODEL_KEY=  
MODEL_URL= 
```

## Usage

### With Claude Desktop

Add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "xiyan": {
      "command": "uv",
      "args": [
        "--directory", 
        "path/to/xiyan_mcp_server",
        "run",
        "xiyan_mcp_server"
      ],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "your_username",
        "MYSQL_PASSWORD": "your_password",
        "MYSQL_DATABASE": "your_database",
        "MODEL_NAME": "your_model_name",
        "MODEL_URL": "your_model enpoint",
        "MODEL_KEY": "your_model_key"
      }
    }
  }
}
```

### As a standalone server

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python -m xiyan_mcp_server
```

## Development

```bash
# Clone the repository
git clone https://github.com/XGenerationLab/xiyan_mcp_server.git
cd xiyan_mcp_server

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -r requirements.txt

```
