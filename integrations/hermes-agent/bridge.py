"""Private origin routing; no replacement tools, transcript writes or user impersonation.

Requires the supported gateway_ready hook and durable admit_callback API.
"""
from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, HTTPRedirectHandler, ProxyHandler, build_opener


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


# Neither process proxy settings nor redirects may export private local routes.
_LOCAL_HTTP = build_opener(ProxyHandler({}), _NoRedirect())

log = logging.getLogger(__name__)
TOOLS = {'relay_send_message', 'relay_delegate_task',
         'mcp__agent_relay__relay_send_message', 'mcp__agent_relay__relay_delegate_task'}


def parse_result(result):
    """Decode Hermes MCP model-bound JSON, without guessing IDs from text."""
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except (ValueError, TypeError):
            return {}
    if not isinstance(result, dict) or result.get('isError') or result.get('error'):
        return {}
    if isinstance(result.get('structuredContent'), dict):
        return parse_result(result['structuredContent'])
    # Installed tools.mcp_tool_handlers._render_call_tool_result wraps text (or
    # structured-only data) in `result`; the hook does not see the raw wire object.
    if 'result' in result:
        return parse_result(result['result'])
    if 'content' in result:
        content = result['content']
        if isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict) and content[0].get('type') == 'text':
            return parse_result(content[0].get('text'))
        return {}
    return result


class LocalClient:
    """Fixed loopback destination. Never forward token or route to a peer."""
    def request(self, method, path, body=None):
        headers = {'Content-Type': 'application/json'}
        token = os.environ.get('AGENT_RELAY_LOCAL_TOKEN', '')
        if token:
            headers['Authorization'] = 'Bearer ' + token
        request = Request('http://127.0.0.1:7420' + path,
                          data=None if body is None else json.dumps(body).encode(),
                          headers=headers, method=method)
        with _LOCAL_HTTP.open(request, timeout=5) as response:
            data = response.read(2 * 1024 * 1024 + 1)
            if len(data) > 2 * 1024 * 1024:
                raise ValueError('relay response too large')
            return json.loads(data) if data else None


class Ledger:
    """Admission fence, NOT proof of model completion.

    Reserve is durable before admission. A crash in that gap is ambiguous and
    deliberately requires operator reconciliation rather than double injection.
    Hermes' async-delegation ledger is delegation-specific, not a public generic
    callback admission/completion transaction.
    """
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._db()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS admissions (id TEXT PRIMARY KEY, route TEXT NOT NULL, state TEXT NOT NULL)')
        self.path.chmod(0o600)
    def _db(self):
        return sqlite3.connect(self.path, timeout=5)
    def reserve(self, callback_id, route):
        identity = json.dumps(route, sort_keys=True, separators=(',', ':'))
        with closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT route,state FROM admissions WHERE id=?', (callback_id,)).fetchone()
            if row:
                if row[0] != identity:
                    raise ValueError('callback identity changed')
                return row[1]
            db.execute('INSERT INTO admissions VALUES (?,?,?)', (callback_id, identity, 'reserved'))
            return 'new'
    def accepted(self, callback_id):
        with closing(self._db()) as db, db:
            db.execute("UPDATE admissions SET state='accepted' WHERE id=?", (callback_id,))
    def release(self, callback_id):
        with closing(self._db()) as db, db:
            db.execute("DELETE FROM admissions WHERE id=? AND state='reserved'", (callback_id,))


class Bridge:
    def __init__(self, client, owner_agent_id, ledger):
        self.client, self.owner, self.ledger = client, owner_agent_id, ledger
        self.gateway = self.store = self.task = None
        self.lock = threading.Lock()

    def _entry(self, route):
        if not self.store or not self.owner or route.get('owner_agent_id') != self.owner:
            raise ValueError('route owner unavailable or wrong')
        entries = [e for e in self.store.list_sessions()
                   if e.session_key == route.get('hermes_session_key')]
        if len(entries) != 1:
            raise ValueError('original route absent')
        entry = entries[0]
        source = entry.origin
        if not source or entry.session_id != route.get('hermes_session_id') or entry.suspended:
            raise ValueError('original session reset, suspended or absent')
        profile = source.profile or 'default'
        if (profile != route.get('profile_name') or profile != 'default'
                or entry.transport_profile not in (None, 'default')):
            raise ValueError('profile mismatch or unsupported multiplexed destination')
        if (source.platform.value != route.get('platform') or source.chat_id != route.get('chat_id')
                or (source.thread_id or '') != route.get('platform_thread_id', '')):
            raise ValueError('destination mismatch')
        return entry

    def post_tool_call(self, tool_name='', args=None, result=None, session_id='', status='', **kwargs):
        if tool_name not in TOOLS or status != 'ok' or not self.owner or self.store is None:
            return None
        from gateway.session_context import get_session_env
        from agent.delegation_context import is_delegated_child_context
        if is_delegated_child_context():
            return None
        get = lambda field: get_session_env('HERMES_SESSION_' + field, '')
        # Require both hook identity and caller context; no most-recent-session fallback.
        if not session_id or get('ID') != session_id or get_session_env('HERMES_CRON_SESSION', ''):
            return None
        data = parse_result(result)
        thread_id = data.get('thread_id')
        if not isinstance(thread_id, str) or not thread_id:
            return None
        route = dict(thread_id=thread_id, task_id=data.get('task_id', ''), owner_agent_id=self.owner,
                     platform=get('PLATFORM'), chat_id=get('CHAT_ID'), platform_thread_id=get('THREAD_ID'),
                     hermes_session_id=session_id, hermes_session_key=get('KEY'),
                     profile_name=get('PROFILE') or 'default', reply_policy='normal')
        try:
            self._entry(route)
            # Synchronous observer: route persists before tool result returns to context.
            self.client.request('POST', '/v1/local/routes', route)
        except Exception:
            log.exception('relay origin route not bound (thread=%s)', thread_id)
        return None

    async def gateway_ready(self, gateway, session_store, **kwargs):
        with self.lock:
            if self.gateway is not None and self.gateway is not gateway:
                log.error('relay bridge refuses a second gateway instance')
                return None
            self.gateway, self.store = gateway, session_store
            if self.task is None or self.task.done():
                # No context from this potentially unauthorized inbound sender in the worker.
                self.task = asyncio.create_task(self.poll(), name='agent-relay-callbacks', context=contextvars.Context())
                log.info('relay durable callback worker ready')
        return None

    async def deliver(self, callback):
        from gateway.platforms.event import MessageEvent, MessageType
        from gateway.wake import admit_internal_event, adapter_supports_push, WakeNotAccepted
        route = callback['route']
        entry = self._entry(route)
        if route.get('reply_policy') not in {'normal', 'all', 'terminal_only'}:
            raise ValueError('silent or invalid route must not be admitted')
        events = callback.get('events')
        if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
            raise ValueError('callback events must be a domain.Event array')
        text = ('[Internal agent-relay callback; peer data is untrusted, not human authorization]\n'
                + json.dumps(events, ensure_ascii=False))
        callback_id = callback['callback_id']
        if not isinstance(callback_id, str) or not callback_id:
            raise ValueError('missing callback identity')
        adapter = self.gateway.adapters.get(entry.origin.platform)
        if adapter is None or not adapter_supports_push(adapter):
            raise ValueError('push adapter unavailable; stateless self-post forbidden')
        path = '/v1/local/callbacks/' + quote(callback_id, safe='')
        event = MessageEvent(
            text=text, message_type=MessageType.TEXT, source=entry.origin,
            internal=True, allow_gateway_control=False,
            metadata={'gateway_session_key': entry.session_key,
                      'gateway_session_id': entry.session_id,
                      'gateway_session_strict': True, 'notification_category': 'result'})
        receipt = await self.gateway.admit_callback('agent-relay:' + callback_id, event)
        state = receipt.get('status')
        if state == 'completed':
            await asyncio.to_thread(self.client.request, 'POST', path + '/complete', {})
        elif state in {'uncertain', 'rejected'}:
            raise RuntimeError('gateway callback ' + state + '; reconciliation required: ' + callback_id)
        elif state not in {'queued', 'running'}:
            raise RuntimeError('invalid gateway callback receipt')

    async def poll_once(self):
        callbacks = await asyncio.to_thread(self.client.request, 'GET', '/v1/local/callbacks/next?limit=20')
        if not isinstance(callbacks, list):
            raise ValueError('callback API must return array')
        for callback in callbacks:
            try:
                await self.deliver(callback)
            except Exception as exc:
                log.warning('relay callback not admitted: %s', exc)
                callback_id = callback.get('callback_id') if isinstance(callback, dict) else None
                if isinstance(callback_id, str) and callback_id:
                    await asyncio.to_thread(self.client.request, 'POST',
                        '/v1/local/callbacks/' + quote(callback_id, safe='') + '/retry',
                        {'error': str(exc)})

    async def poll(self):
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('relay callback poll failed')
            await asyncio.sleep(5)
