# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in Dust, please report it responsibly:

**Email:** security@dustdb.dev

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Scope

Security issues in the following areas are in scope:
- SQL injection or parser bypasses
- Storage engine corruption (checksum bypasses, WAL replay issues)
- Path traversal in project initialization or branch operations
- Memory safety issues

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.x     | Current development — fixes applied to main |

## Disclosure policy

We follow coordinated disclosure. We ask that you give us reasonable time to address the issue before public disclosure.
