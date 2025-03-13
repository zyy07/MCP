#from . import server
from server import *

def main():
   """Main entry point for the package."""
   mcp.run()

# Expose important items at package level
__all__ = ['main', 'server']