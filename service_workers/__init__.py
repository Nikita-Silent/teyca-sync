"""One-off and remediation scripts that do not run continuously in prod.

Kept out of app/ so they are excluded from the app-scoped quality gates
(coverage, complexity, docstring coverage, dead-code, security scan) that
apply to services actually running in the compose stack.
"""
