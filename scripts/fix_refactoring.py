#!/usr/bin/env python3
"""
Fix the refactoring script to properly extract decorators and constants.

This script will:
1. Update find_function_boundaries to include decorators
2. Add missing constants to CONSTANT_STRUCTURE
3. Re-extract functions with decorators
"""

import re
from pathlib import Path


def fix_find_function_boundaries():
    """
    Fix the find_function_boundaries function to include decorators.
    
    The current version only looks for 'def' at column 0, missing all decorators.
    We need to scan backwards from each function to capture decorators.
    """
    new_function = '''def find_function_boundaries(lines):
    """Find start and end line numbers for all functions, INCLUDING decorators."""
    boundaries = {}
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for function definition at column 0
        match = re.match(r'^(async )?def ([a-zA-Z_][a-zA-Z0-9_]*)\(', line)
        if match:
            func_name = match.group(2)
            
            # CRITICAL FIX: Scan backwards to find decorators
            # Decorators are lines starting with @ at column 0
            start = i
            while start > 0:
                prev_line = lines[start - 1].rstrip()
                # Check if previous line is a decorator or blank
                if not prev_line:  # blank line
                    start -= 1
                    continue
                elif prev_line.startswith('@'):  # decorator
                    start -= 1
                    continue
                else:
                    # Hit something that's not a decorator or blank - stop
                    break
            
            # Find end of function signature
            i += 1
            while i < len(lines):
                if ')' in lines[i] and ':' in lines[i]:
                    i += 1
                    break
                i += 1
            
            # Skip blank lines after signature
            while i < len(lines) and not lines[i].strip():
                i += 1
            
            if i < len(lines):
                # Get the base indentation from the first line of function body
                base_indent = len(lines[i]) - len(lines[i].lstrip())
                
                # Scan forward to find end of function
                while i < len(lines):
                    curr_line = lines[i]
                    
                    # Empty lines are part of the function
                    if not curr_line.strip():
                        i += 1
                        continue
                    
                    # If we hit column 0, function has ended
                    curr_indent = len(curr_line) - len(curr_line.lstrip())
                    if curr_indent == 0:
                        break
                    
                    i += 1
                
                # Include trailing blank lines
                end = i
                while end < len(lines) and not lines[end].strip():
                    end += 1
                
                boundaries[func_name] = (start, end)
                continue
        
        i += 1
    
    return boundaries
'''
    return new_function


def fix_constant_structure():
    """Add missing constants to CONSTANT_STRUCTURE."""
    return '''CONSTANT_STRUCTURE = {
    "shared.py": [
        "MAX_RESPONSE_SIZE",
        "WARNING_SIZE",
    ],
    "pq.py": ["DEFAULT_PQ_TIMEOUT", "DEFAULT_MAX_CONCURRENT"],
    "session_community.py": [
        "DEFAULT_LAUNCH_METHOD",
        "DEFAULT_AUTH_TYPE",
        "DEFAULT_DOCKER_IMAGE_PYTHON",
        "DEFAULT_DOCKER_IMAGE_GROOVY",
        "DEFAULT_HEAP_SIZE_GB",
        "DEFAULT_STARTUP_TIMEOUT_SECONDS",
        "DEFAULT_STARTUP_CHECK_INTERVAL_SECONDS",
        "DEFAULT_STARTUP_RETRIES",
    ],
}
'''


def main():
    script_path = Path("scripts/refactor_split_mcp.py")
    
    # Read the current script
    with open(script_path, 'r') as f:
        content = f.read()
    
    # Fix 1: Replace find_function_boundaries
    # Find the function and replace it
    pattern = r'def find_function_boundaries\(lines\):.*?return boundaries'
    new_func = fix_find_function_boundaries()
    content = re.sub(pattern, new_func, content, flags=re.DOTALL)
    
    # Fix 2: Replace CONSTANT_STRUCTURE
    pattern = r'CONSTANT_STRUCTURE = \{[^}]+\}'
    new_const = fix_constant_structure()
    content = re.sub(pattern, new_const, content, flags=re.DOTALL)
    
    # Write back
    with open(script_path, 'w') as f:
        f.write(content)
    
    print("✅ Fixed refactor_split_mcp.py:")
    print("   - find_function_boundaries now extracts decorators")
    print("   - CONSTANT_STRUCTURE now includes shared.py constants")
    print("\nNext steps:")
    print("   1. Re-run refactor_split_mcp.py to re-extract with decorators")
    print("   2. Verify all decorators present in extracted files")
    print("   3. Remove manually added constants from shared.py")


if __name__ == "__main__":
    main()
