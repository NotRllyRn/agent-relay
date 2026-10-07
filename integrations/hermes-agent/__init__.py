"""Hermes agent-relay origin bridge (hooks only)."""
import logging
import os
from pathlib import Path

try:
    from .bridge import Bridge, LocalClient
except ImportError:  # Supports file-based plugin loaders as well as packages.
    import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location('agent_relay_bridge_impl', Path(__file__).with_name('bridge.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    Bridge, LocalClient = module.Bridge, module.LocalClient


def register(ctx):

    owner = os.environ.get('AGENT_RELAY_AGENT_ID', '')
    if not owner:
        logging.getLogger(__name__).warning('AGENT_RELAY_AGENT_ID unset: relay route binding fails closed')
    bridge = Bridge(LocalClient(), owner)
    ctx.register_hook('transform_tool_result', bridge.transform_tool_result)
    ctx.register_hook('gateway_ready', bridge.gateway_ready)
