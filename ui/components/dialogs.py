"""Where a dialog must be parented.

NiceGUI runs an event handler "within the context of the parent slot of the sender"
(``nicegui/events.py`` ``handle_event``). So a ``ui.dialog()`` created inside a click
handler lands in whatever container held the button — and if that container is cleared
by a refresh, or is itself a dialog that the handler just closed, the new dialog exists
but can never paint. Two shipped bugs came from exactly this:

  · the Journey's rail rebuild and the Species page's rev-gated Picks table destroyed
    dialogs mid-interaction ("parent element ... has been deleted"), and
  · the aggregate dialog's "extracted particles instead" link closed itself and opened
    the merge dialog INSIDE its own closing card, so the click read as a no-op.

``client.layout.default_slot`` lives for the whole page and is never cleared, so it is
the right host for anything modal. ``nullcontext()`` when there is no client layout (a
background task, a non-page context): the caller's current slot is then the only option.

Usage — wrap the dialog's construction, not its ``open()``::

    with dialog_host(), ui.dialog() as dlg, ui.card():
        ...
"""

from __future__ import annotations

from contextlib import nullcontext

from nicegui import context


def dialog_host():
    """The slot a dialog must be parented at: the page LAYOUT slot, never the element
    that opened it."""
    try:
        return context.client.layout.default_slot
    except (RuntimeError, AttributeError):
        return nullcontext()
