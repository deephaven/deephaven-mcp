#!/usr/bin/env python3
"""Fix tests to use directly imported private helpers instead of module prefixes."""

import re
from pathlib import Path

def fix_test_file(filepath):
    """Fix a single test file to use directly imported functions."""
    content = filepath.read_text()
    original = content
    
    # Extract module name from filepath (e.g., test_pq.py -> pq)
    module_name = filepath.stem.replace('test_', '')
    module_prefix = f'{module_name}_mod.'
    
    # Find all private function names imported from this module's _tools
    # Pattern: from deephaven_mcp.mcp_systems_server._tools.{module} import (..., _func_name, ...)
    import_pattern = rf'from deephaven_mcp\.mcp_systems_server\._tools\.{module_name} import \('
    match = re.search(import_pattern + r'([^)]+)\)', content, re.DOTALL)
    
    if not match:
        return False
    
    # Extract imported names
    imports_block = match.group(1)
    imported_funcs = []
    for line in imports_block.split('\n'):
        line = line.strip()
        if line and not line.startswith('#'):
            # Extract function name (handle trailing commas)
            func = line.rstrip(',').strip()
            if func.startswith('_'):
                imported_funcs.append(func)
    
    if not imported_funcs:
        return False
    
    # Replace module_prefix._func with just _func for all imported private functions
    for func in imported_funcs:
        # Match word boundaries to avoid partial replacements
        pattern = rf'\b{module_prefix}{func}\b'
        replacement = func
        content = re.sub(pattern, replacement, content)
    
    # Also handle attribute access (e.g., module_mod._var = {})
    # Pattern: module_prefix followed by any underscore-prefixed name
    content = re.sub(
        rf'\b{re.escape(module_prefix)}(_[a-zA-Z_][a-zA-Z0-9_]*)\b',
        r'\1',
        content
    )
    
    if content != original:
        filepath.write_text(content)
        print(f"✅ Fixed {filepath.name}: replaced {module_prefix}* with direct calls")
        return True
    return False

def main():
    """Fix all test files in _tools directory."""
    test_dir = Path('tests/mcp_systems_server/_tools')
    
    if not test_dir.exists():
        print(f"❌ Directory not found: {test_dir}")
        return
    
    print("Fixing private helper calls in test files...")
    fixed_count = 0
    
    for test_file in sorted(test_dir.glob('test_*.py')):
        if fix_test_file(test_file):
            fixed_count += 1
    
    print(f"\n✅ Fixed {fixed_count} test files")

if __name__ == '__main__':
    main()
