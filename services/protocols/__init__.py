"""Protocols — portable, declarative captures of a workflow that worked (roadmap 14).

    schema.py         Protocol / ProtocolSpecies / ProtocolStage + yaml load/dump
    discovery.py      config/protocols/ + ~/.crboost/protocols/ lookup
    export.py         project -> Protocol (the authoring tool)
    apply.py          Protocol -> new project (the one engine; harness + landing action)
    scheme_export.py  Protocol + applied project -> vanilla RELION Schemes/<name>/

Import from the submodules; this package init stays empty on purpose.
"""
