---
trigger: always_on
---

# General programming practices that should be obeyed.
1. A Python file should not access private variables, functions, or methods in another file or package.  It is ok for the test file for a package to access and use the package being tested, even if it is private, and it is ok for the test file to access private variables, functions, and methods in the package.
2. There should be a one-to-one correspondance between source files and test files.  Unless there is a strongly compelling reason, all tests for a python source file should be in a single test file.