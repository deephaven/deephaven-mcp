---
trigger: glob
globs: *.md
---

# Documentation Standards

## JSON/JSON5 Code Blocks in Markdown

1. **Use only ASCII characters in JSON/JSON5 code blocks**: JSON examples in markdown files must contain only ASCII characters (character codes 0-127). Do not use emoji or other non-ASCII Unicode characters (such as ✅, ❌, ⚠️) within JSON/JSON5 code blocks, even in comments.

   **Rationale**: Users copy-paste these examples into configuration files. Many JSON parsers and text editors have issues with non-ASCII characters, leading to parsing errors.

   **Correct**:
   ```json5
   {
     "auth_type": "password",
     "username": "admin",
     "password": "your-password-here"  // RECOMMENDED: Use password_env_var for security
   }
   ```

   **Incorrect**:
   ```json5
   {
     "auth_type": "password",
     "username": "admin",
     "password": "your-password-here"  // ✅ RECOMMENDED: Use password_env_var for security
   }
   ```

2. **Use ```json5 for code blocks with comments**: If a JSON code block contains comments (// or /* */), mark it as ```json5, not ```json. Standard JSON does not support comments.

   **Correct**:
   ```json5
   // This is a valid JSON5 comment
   {
     "key": "value"  // Inline comment
   }
   ```

   **Incorrect**:
   ```json
   // This will cause a parser error
   {
     "key": "value"
   }
   ```

3. **Validate JSON blocks**: All code blocks marked as ```json must be valid, parseable JSON. Use a JSON validator to verify examples before committing.

## General Documentation Standards

1. **Keep examples copy-paste ready**: All code examples should work when copied directly from documentation without modification (except for placeholder values like passwords, URLs, etc.).

2. **Use clear placeholder values**: Make it obvious what values need to be replaced:
   - Good: `"password": "your-password-here"`, `"host": "your-server.example.com"`
   - Bad: `"password": "xxxx"`, `"host": "server"`

3. **Document placeholder requirements**: When using placeholders, add comments explaining what format is expected (e.g., `"auth_token": "username:password"  // Must be in "user:pass" format`).
