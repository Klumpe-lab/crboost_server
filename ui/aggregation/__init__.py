"""Aggregation UI: the cross-project merge card.

``merge_card`` is the whole surface — discover source projects, pick the species, review the
stale-source check, and run the merge. It reads its facts through
``services/aggregation/`` and never touches the merge driver directly.
"""
