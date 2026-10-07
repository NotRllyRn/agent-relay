import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, '/home/hermes/.hermes/hermes-agent')
from gateway.config import Platform
from gateway.session import SessionSource, SessionEntry
from gateway.session_context import set_session_vars, clear_session_vars
from gateway.wake import WakeNotAccepted
from bridge import Bridge, parse_result

class Client:
    def __init__(self): self.calls = []; self.callbacks = []
    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        return self.callbacks if method == 'GET' else {}

class Adapter:
    supports_async_delivery = True
    def __init__(self, accept=True): self.events = []; self.accept = accept
    async def handle_message(self, event):
        self.events.append(event)
        event._gateway_accepted = self.accept

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.client = Client()
        self.bridge = Bridge(self.client, 'local')
        self.source = SessionSource(platform=Platform.TELEGRAM, chat_id='42', thread_id='7', profile='default')
        now = datetime.now()
        self.entry = SessionEntry('agent:main:telegram:42:7', 'sid', now, now, origin=self.source)
        self.adapter = Adapter()
        self.store = SimpleNamespace(list_sessions=lambda: [self.entry])
        self.gateway = SimpleNamespace(adapters={Platform.TELEGRAM: self.adapter}, session_store=self.store)
        self.receipts = {}
        async def admit(callback_id, event):
            if not self.adapter.supports_async_delivery:
                raise ValueError('push adapter unavailable')
            if callback_id not in self.receipts:
                await self.adapter.handle_message(event)
                if not event._gateway_accepted:
                    raise WakeNotAccepted('not admitted')
                self.receipts[callback_id] = {'callback_id':callback_id, 'status':'completed'}
            return self.receipts[callback_id]
        self.gateway.admit_callback = admit
        self.bridge.gateway = self.gateway
        self.bridge.store = self.store
        self.route = dict(thread_id='thr', task_id='', owner_agent_id='local', platform='telegram', chat_id='42', platform_thread_id='7', hermes_session_id='sid', hermes_session_key=self.entry.session_key, profile_name='default', reply_policy='normal')
        self.route['transport_profile'] = 'default'
        self.callback = dict(callback_id='cb', route=self.route, events=[dict(event_id='evt', event_type='message.created', payload={'body_markdown':'reply'})], attempts=0)
    def tearDown(self):
        clear_session_vars([])
        self.tmp.cleanup()
    def bind(self, result, **extra):
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        return self.bridge.post_tool_call(tool_name='mcp__agent_relay__relay_send_message', args={'recipient':'peer'}, result=result, session_id='sid', status='ok', **extra)
    def test_bind_direct_hook_preserves_result(self):
        result = json.dumps({'thread_id':'thr','message_id':'msg'})
        self.assertIsNone(self.bind(result))
        self.assertEqual(self.client.calls[0], ('POST', '/v1/local/routes', self.route))

    def test_model_receives_binding_receipt_without_private_destination(self):
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        result = json.dumps({'thread_id':'thr', 'message_id':'msg'})
        out = self.bridge.transform_tool_result(tool_name='mcp__agent_relay__relay_send_message', result=result, session_id='sid', status='ok')
        decoded = json.loads(out)
        self.assertEqual(decoded['message_id'], 'msg')
        self.assertTrue(decoded['agent_relay_return_route']['bound'])
        self.assertNotIn('chat_id', decoded['agent_relay_return_route'])
        self.assertNotIn('hermes_session_id', decoded['agent_relay_return_route'])
    def test_secondary_runtime_and_receiving_bot_preserve_origin(self):
        self.source.profile = 'research'
        self.entry.transport_profile = 'maya'
        self.entry.session_key = 'agent:research:telegram:42:7'
        self.route.update(profile_name='research', hermes_session_key=self.entry.session_key, transport_profile='maya')
        asyncio.run(self.bridge.deliver(self.callback))
        self.assertEqual(self.adapter.events[0].source.profile, 'research')
        self.assertEqual(self.adapter.events[0].metadata['gateway_session_key'], self.entry.session_key)
    def test_receiving_bot_changed_before_first_reply_rejected(self):
        self.entry.transport_profile = 'other'
        with self.assertRaises(ValueError):
            asyncio.run(self.bridge.deliver(self.callback))
        self.assertFalse(self.adapter.events)
    def test_mcp_content_and_structured_result(self):
        self.assertEqual(parse_result({'content':[{'type':'text','text':'{"thread_id":"thr"}'}]}), {'thread_id':'thr'})
        self.assertEqual(parse_result({'structuredContent':{'thread_id':'thr'}}), {'thread_id':'thr'})
    def test_installed_mcp_renderer_and_hook_caller(self):
        from mcp.types import CallToolResult, TextContent
        from tools.mcp_tool_handlers import _render_call_tool_result
        from model_tools import _emit_post_tool_call_hook
        from unittest.mock import patch
        raw = {'thread_id':'thr', 'message_id':'msg'}
        rendered = _render_call_tool_result(CallToolResult(content=[TextContent(type='text', text=json.dumps(raw))], structuredContent=raw), 'agent_relay')
        self.assertEqual(parse_result(rendered), raw)
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        def dispatch(name, **kw):
            self.assertEqual(name, 'post_tool_call')
            self.bridge.post_tool_call(**kw)
        with patch('hermes_cli.lifecycle.has_hook', return_value=True), patch('hermes_cli.lifecycle.invoke_hook', side_effect=dispatch):
            _emit_post_tool_call_hook(function_name='mcp__agent_relay__relay_send_message', function_args={'recipient':'peer'}, result=rendered, session_id='sid', status='ok')
        self.assertEqual(self.client.calls[0][2], self.route)
    def test_no_context_no_route(self):
        out = self.bridge.transform_tool_result(tool_name='mcp__agent_relay__relay_send_message', result='{"thread_id":"thr"}', session_id='sid', status='ok')
        assert isinstance(out, str)
        self.assertFalse(json.loads(out)['agent_relay_return_route']['bound'])
        self.bridge.post_tool_call(tool_name='mcp__agent_relay__relay_send_message', args={}, result='{"thread_id":"thr"}', session_id='sid', status='ok')
        self.assertFalse(self.client.calls)
    def test_unavailable_owner_or_store_receipt(self):
        for field, value in [('owner', ''), ('store', None)]:
            original = getattr(self.bridge, field)
            setattr(self.bridge, field, value)
            out = self.bridge.transform_tool_result(tool_name='relay_send_message', result='{"thread_id":"thr"}', status='ok')
            assert isinstance(out, str)
            self.assertFalse(json.loads(out)['agent_relay_return_route']['bound'])
            setattr(self.bridge, field, original)
        self.assertFalse(self.client.calls)
    def test_context_session_mismatch(self):
        set_session_vars(platform='telegram', chat_id='42', session_key=self.entry.session_key, session_id='other', profile='default')
        out = self.bridge.transform_tool_result(tool_name='relay_send_message', result='{"thread_id":"thr"}', session_id='sid', status='ok')
        assert isinstance(out, str)
        self.assertFalse(json.loads(out)['agent_relay_return_route']['bound'])
        self.bridge.post_tool_call(tool_name='mcp__agent_relay__relay_send_message', args={}, result='{"thread_id":"thr"}', session_id='sid', status='ok')
        self.assertFalse(self.client.calls)
    def test_child_and_cron_negative_receipts(self):
        from unittest.mock import patch
        for target in ('agent.delegation_context.is_delegated_child_context', 'gateway.session_context.get_session_env'):
            with self.subTest(target=target), patch(target, return_value=True):
                out = self.bridge.transform_tool_result(tool_name='relay_send_message', result='{"thread_id":"thr"}', session_id='sid', status='ok')
                assert isinstance(out, str)
                self.assertFalse(json.loads(out)['agent_relay_return_route']['bound'])
        self.assertFalse(self.client.calls)
        self.assertIsNone(self.bridge.transform_tool_result(tool_name='unrelated', result='{"thread_id":"thr"}', status='ok'))
        self.assertIsNone(self.bridge.transform_tool_result(tool_name='relay_send_message', result='{"error":"failed"}', status='ok'))
    def test_admission_actual_event_and_durable_dedupe(self):
        asyncio.run(self.bridge.deliver(self.callback))
        event = self.adapter.events[0]
        self.assertTrue(event.internal)
        self.assertFalse(event.allow_gateway_control)
        self.assertTrue(event.metadata['gateway_session_strict'])
        self.assertEqual(event.metadata['gateway_session_id'], 'sid')
        self.assertEqual(event.metadata['gateway_session_key'], self.entry.session_key)
        self.assertEqual(event.source, self.source)
        self.assertIn('at most 150 words', event.text)
        self.assertIn('terminal', event.text)
        self.assertIn('Do not start new work', event.text)
        # Simulate process restart with the same durable ledger.
        other = Bridge(self.client, 'local')
        other.gateway, other.store = self.gateway, self.store
        asyncio.run(other.deliver(self.callback))
        self.assertEqual(len(self.adapter.events), 1)
    def test_installed_gateway_strict_guard_rejects_rotation(self):
        from gateway.run_turn import GatewayTurnMixin
        from unittest.mock import AsyncMock
        asyncio.run(self.bridge.deliver(self.callback))
        event = self.adapter.events[0]
        self.entry.session_id = 'rotated'
        runner = SimpleNamespace(
            _session_key_for_source=lambda source: self.entry.session_key,
            async_session_store=SimpleNamespace(lookup_by_session_key=AsyncMock(return_value=self.entry)))
        outcome = asyncio.run(GatewayTurnMixin._hmwa_resolve_session(runner, event, self.source))
        self.assertIsNone(outcome)
        self.assertEqual(len(self.adapter.events), 1)
    def test_stale_identity_and_private_owner_fail_closed(self):
        for field in ('owner_agent_id','platform','chat_id','platform_thread_id','hermes_session_id','hermes_session_key','profile_name'):
            cb = {**self.callback, 'route':{**self.route, field:'wrong'}}
            with self.subTest(field=field), self.assertRaises(ValueError):
                asyncio.run(self.bridge.deliver(cb))
        self.assertFalse(self.adapter.events)
    def test_unaccepted_not_completed_and_retryable(self):
        self.adapter.accept = False
        with self.assertRaises(WakeNotAccepted): asyncio.run(self.bridge.deliver(self.callback))
        self.assertFalse(any('/complete' in path for _,path,_ in self.client.calls))
        self.adapter.accept = True
        asyncio.run(self.bridge.deliver(self.callback))
        self.assertEqual(len(self.adapter.events), 2)

    def test_queued_receipt_is_not_completion(self):
        self.receipts['agent-relay:cb'] = {'callback_id':'agent-relay:cb', 'status':'queued'}
        asyncio.run(self.bridge.deliver(self.callback))
        self.assertFalse(any('/complete' in path for _,path,_ in self.client.calls))
        self.receipts['agent-relay:cb']['status'] = 'completed'
        asyncio.run(self.bridge.deliver(self.callback))
        self.assertTrue(any('/complete' in path for _,path,_ in self.client.calls))
    def test_stateless_and_suspended_fail_closed(self):
        self.adapter.supports_async_delivery = False
        with self.assertRaises(ValueError): asyncio.run(self.bridge.deliver(self.callback))
        self.adapter.supports_async_delivery = True
        self.entry.suspended = True
        with self.assertRaises(ValueError): asyncio.run(self.bridge.deliver(self.callback))
    def test_actual_dispatch_preserves_tool_result_and_caller_context(self):
        import model_tools
        from tools.registry import registry
        from unittest.mock import patch
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        rendered = json.dumps({'result':json.dumps({'thread_id':'thr', 'message_id':'msg'})})
        def dispatch_hook(name, **kw):
            self.assertEqual(name, 'post_tool_call')
            self.bridge.post_tool_call(**kw)
            return []
        with patch.object(registry, 'dispatch', return_value=rendered), patch('hermes_cli.lifecycle.has_hook', side_effect=lambda name: name == 'post_tool_call'), patch('hermes_cli.lifecycle.invoke_hook', side_effect=dispatch_hook):
            out = model_tools.handle_function_call('mcp__agent_relay__relay_send_message', {'recipient':'peer'}, session_id='sid', tool_call_id='tc', skip_pre_tool_call_hook=True, skip_tool_execution_middleware=True)
        self.assertEqual(out, rendered)
        self.assertEqual(self.client.calls[0][2], self.route)
    def test_actual_dispatch_returns_binding_receipt(self):
        import model_tools
        from tools.registry import registry
        from unittest.mock import patch
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        rendered = json.dumps({'result':json.dumps({'thread_id':'thr', 'message_id':'msg'})})
        def dispatch_hook(name, **kw):
            return [self.bridge.transform_tool_result(**kw)]
        with patch.object(registry, 'dispatch', return_value=rendered), patch('hermes_cli.lifecycle.has_hook', side_effect=lambda name: name == 'transform_tool_result'), patch('hermes_cli.lifecycle.invoke_hook', side_effect=dispatch_hook):
            out = model_tools.handle_function_call('mcp__agent_relay__relay_send_message', {'recipient':'peer'}, session_id='sid', tool_call_id='tc', skip_pre_tool_call_hook=True, skip_tool_execution_middleware=True)
        self.assertEqual(parse_result(out), {'thread_id':'thr', 'message_id':'msg'})
        self.assertTrue(json.loads(out)['agent_relay_return_route']['bound'])
        self.assertEqual(self.client.calls[0][2], self.route)
    def test_local_http_fixed_loopback_and_token(self):
        import io
        import bridge
        from unittest.mock import patch
        with patch.dict('os.environ', {'AGENT_RELAY_LOCAL_TOKEN':'test-token'}), patch.object(bridge._LOCAL_HTTP, 'open', return_value=io.BytesIO(b'[]')) as opened:
            self.assertEqual(bridge.LocalClient().request('GET', '/v1/local/callbacks/next?limit=20'), [])
            request = opened.call_args.args[0]
            self.assertEqual(request.full_url, 'http://127.0.0.1:7420/v1/local/callbacks/next?limit=20')
            self.assertEqual(request.get_header('Authorization'), 'Bearer test-token')
        self.assertIsNone(bridge._NoRedirect().redirect_request(None,None,302,'',{},'https://peer.example/'))
    def test_delegate_binds_task_and_errors_do_not_bind(self):
        set_session_vars(platform='telegram', chat_id='42', thread_id='7', session_key=self.entry.session_key, session_id='sid', profile='default')
        self.bridge.post_tool_call(tool_name='mcp__agent_relay__relay_delegate_task', args={'recipient':'peer'}, result=json.dumps({'result':{'thread_id':'thr','task_id':'task','created_by':'local'}}), session_id='sid', status='ok')
        self.assertEqual(self.client.calls[0][2], {**self.route, 'task_id':'task'})
        self.client.calls.clear()
        self.bridge.post_tool_call(tool_name='mcp__agent_relay__relay_send_message', args={}, result='{"thread_id":"thr"}', session_id='sid', status='error')
        self.assertFalse(self.client.calls)
    def test_poll_api_complete_and_retry_contract(self):
        self.client.callbacks = [self.callback]
        asyncio.run(self.bridge.poll_once())
        self.assertIn(('GET', '/v1/local/callbacks/next?limit=20', None), self.client.calls)
        self.assertIn(('POST', '/v1/local/callbacks/cb/complete', {}), self.client.calls)
        self.client.calls.clear()
        self.client.callbacks = [{**self.callback, 'callback_id':'bad', 'route':{**self.route, 'hermes_session_id':'stale'}}]
        asyncio.run(self.bridge.poll_once())
        self.assertEqual(self.client.calls[-1][1], '/v1/local/callbacks/bad/retry')
        self.assertIn('error', self.client.calls[-1][2])
    def test_lazy_bootstrap_one_worker_no_event_identity(self):
        from unittest.mock import AsyncMock
        async def run():
            self.bridge.gateway = self.bridge.store = None
            self.bridge.poll = AsyncMock()
            await self.bridge.gateway_ready(self.gateway, self.store)
            task = self.bridge.task
            await self.bridge.gateway_ready(self.gateway, self.store)
            self.assertIs(self.bridge.task, task)
            await task
            self.bridge.poll.assert_awaited_once()
        asyncio.run(run())
    def test_complete_failure_replayed_without_duplicate_admission(self):
        original = self.client.request
        def fail(method, path, body=None):
            if path.endswith('/complete'): raise OSError('HTTP unavailable')
            return original(method, path, body)
        self.client.request = fail
        with self.assertRaises(OSError): asyncio.run(self.bridge.deliver(self.callback))
        self.client.request = original
        asyncio.run(self.bridge.deliver(self.callback))
        self.assertEqual(len(self.adapter.events), 1)
    def test_registration_does_not_change_tools(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('relay_plugin', ROOT / '__init__.py')
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
        hooks = {}
        from unittest.mock import patch
        with patch('hermes_constants.get_hermes_home', return_value=Path(self.tmp.name)):
            mod.register(SimpleNamespace(register_hook=lambda name, fn: hooks.update({name:fn})))
        self.assertEqual(set(hooks), {'transform_tool_result','gateway_ready'})

if __name__ == '__main__': unittest.main()
