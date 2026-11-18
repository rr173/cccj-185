"""Offline replication: origin identity, prefix digests, packages, replicas.

Modules:
  identity   exact logical-prefix digest definition and revision units
  origin     source/replica lifecycle, role guards and status reporting
  package    self-describing framed offline package wire format (v1)
  exporter   single-snapshot, streaming ``(after, through]`` package export
  replica    read-only package verdicts and atomic whole-package acceptance
"""
