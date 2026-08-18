"""Aggregation UI (de-novo roadmap S6): the cross-project merge card.

``merge_card`` is the whole surface — discover source projects, pick the species, review the
authoritative-list gate, and submit the merge. It reads its facts through
``services/aggregation/`` and never touches the merge driver directly.

Relocated from ``ui/aggregation_merge_card.py`` by S6, no shim left behind.
"""
