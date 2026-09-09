"""Protocols — the shape of a pipeline with its parameters, as a portable bundle (roadmap 16).

    schema.py         Protocol / ProtocolSpecies / ProtocolStage + yaml load/dump
    discovery.py      config/protocols/ + ~/.crboost/protocols/ lookup
    export.py         project -> Protocol (the authoring tool)
    apply.py          Protocol -> a new, regular project (the one engine)

Import from the submodules; this package init stays empty on purpose.
"""
