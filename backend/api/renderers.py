"""Fixed JSON output for the WMDR2 record endpoint.

A WNM's integrity hash (api/wnm.py) is computed over render_wmdrv2(record),
so the bytes a subscriber downloads from the record link must be exactly
these, whatever the client asks for."""

from rest_framework.renderers import JSONRenderer


class CompactJSONRenderer(JSONRenderer):
    """DRF's compact JSON, ignoring the Accept header's indent parameter."""

    def get_indent(self, accepted_media_type, renderer_context):
        return None


def render_wmdrv2(wmdrv2):
    return CompactJSONRenderer().render(wmdrv2)
