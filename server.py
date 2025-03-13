from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Simple Server")

@mcp.tool()
def add(a: int, b: int) -> int:
    """Add两个数字"""
    return a + b

@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """获取个性化问候"""
    return f"Hello, {name}!"
