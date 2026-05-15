"""Codegen package: convert recorder traces into bookmaker-specific YAML.

Modules added incrementally:
- fanduel: trace -> selectors/fanduel_markets.yaml entries
- betmgm:  trace -> selectors/betmgm_markets.yaml entries
- drift:   diff a fresh trace against stored YAML
"""
