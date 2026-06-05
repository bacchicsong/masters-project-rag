"""
Shared fixtures for all tests.
"""
import sys
from pathlib import Path

# Add src to path so tests can import project modules
SRC_DIR = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_DIR.resolve()))