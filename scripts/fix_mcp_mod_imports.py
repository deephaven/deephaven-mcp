#!/usr/bin/env python3
"""Remove mcp_mod imports and replace mcp_mod.func calls with direct func calls."""

import re
from pathlib import Path

def fix_test_file(filepath):
    """Fix a single test file to use direct function calls instead of mcp_mod."""
    content = filepath.read_text()
    original = content
    
    # Remove the mcp_mod import line
    content = re.sub(
        r'^import deephaven_mcp\.mcp_systems_server\._mcp as mcp_mod\n',
        '',
        content,
        flags=re.MULTILINE
    )
    
    # Find all functions imported from the _tools module for this test file
    # Extract module name from filepath (e.g., test_pq.py -> pq)
    module_name = filepath.stem.replace('test_', '')
    
    # Find the import block for this module's _tools
    import_pattern = rf'from deephaven_mcp\.mcp_systems_server\._tools\.{module_name} import \(([^)]+)\)'
    match = re.search(import_pattern, content, re.DOTALL)
    
    if match:
        # Extract imported function names
        imports_block = match.group(1)
        imported_funcs = []
        for line in imports_block.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                func = line.rstrip(',').strip()
                if func and not func.startswith('_'):  # Public functions
                    imported_funcs.append(func)
        
        # Replace mcp_mod.func with just func for all imported public functions
        for func in imported_funcs:
            pattern = rf'\bmcp_mod\.{func}\b'
            replacement = func
            content = re.sub(pattern, replacement, content)
    
    # Also handle any remaining mcp_mod references (should be none if imports are complete)
    remaining_mcp_mod = re.findall(r'\bmcp_mod\.\w+', content)
    if remaining_mcp_mod:
        print(f"  ⚠️  Warning: {filepath.name} still has mcp_mod references: {set(remaining_mcp_mod)}")
    
    if content != original:
        filepath.write_text(content)
        print(f"✅ Fixed {filepath.name}: removed mcp_mod import and replaced calls")
        return True
    return False

def main():
    """Fix all test files in _tools directory."""
    test_dir = Path('tests/mcp_systems_server/_tools')
    
    if not test_dir.exists():
        print(f"❌ Directory not found: {test_dir}")
        return
    
    print("Removing mcp_mod imports from test files...")
    fixed_count = 0
    
    for test_file in sorted(test_dir.glob('test_*.py')):
        if fix_test_file(test_file):
            fixed_count += 1
    
    print(f"\n✅ Fixed {fixed_count} test files")

if __name__ == '__main__':
    main()
