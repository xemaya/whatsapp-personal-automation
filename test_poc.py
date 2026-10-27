import copy
import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import io
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from fixtures import demo_events
from matcher import extract, match_pair
from server import App, handler_for
from store import DEMO_GROUPS, Store, private_id


class MatchingTests(unittest.TestCase):
    def pair(self, sale, want):
        return match_pair({'key': 's', 'sender_id': 's', 'extracted': extract(sale)},
                          {'key': 'w', 'sender_id': 'w', 'extracted': extract(want)})

    def test_offer_wanted_and_budget(self):
        self.assertIsNotNone(self.pair('出售沙发，伦敦 £80', '求购沙发，伦敦预算 £100'))

    def test_english(self):
        self.assertIsNotNone(self.pair('Selling a desk London £30', 'WTB desk London £50'))

    def test_budget_conflict(self):
        self.assertIsNone(self.pair('出售沙发 £120', '求购沙发 £100'))

    def test_city_conflict(self):
        self.assertIsNone(self.pair('出售沙发 伦敦 £80', '求购沙发 曼城 £100'))

    def test_currency_conflict(self):
        self.assertIsNone(self.pair('出售沙发 £80', '求购沙发 $100'))

    def test_model_conflict(self):
        self.assertIsNone(self.pair('出售 iPhone 14 Pro £300', '求购 iPhone 15 Pro £500'))

    def test_bare_model_is_not_price(self):
        self.assertIsNone(extract('出售 iPhone 14 Pro')['price'])

    def test_multiple_items_are_review(self):
        self.assertIsNone(extract('出售沙发和书桌 £100')['item'])

    def test_closed_message_inactive(self):
        self.assertEqual(extract('沙发已售 £80')['intent'], 'inactive')

    def test_negation(self):
        self.assertNotEqual(extract('不是出售沙发')['intent'], 'offer')

    def test_missing_price_is_explicit(self):
        self.assertIn('价格或预算未说明', self.pair('出售沙发 伦敦', '求购沙发 伦敦')['unknown'])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.s = Store(':memory:')
        self.event = demo_events()[0]

    def tearDown(self):
        self.s.db.close()

    def test_end_to_end_demo(self):
        result = self.s.ingest(self.event, 'demo')
        self.assertEqual(result['accepted'], 9)
        self.assertEqual(result['ignored'], 2)
        self.assertEqual(len(self.s.snapshot()['matches']), 2)

    def test_duplicate_callback_and_new_envelope(self):
        self.s.ingest(self.event, 'demo')
        self.assertTrue(self.s.ingest(self.event, 'demo')['event_duplicate'])
        changed = copy.deepcopy(self.event)
        changed['trace'] = 'another webhook delivery'
        self.s.ingest(changed, 'demo')
        self.assertEqual(len(self.s.snapshot()['messages']), 9)
        self.assertEqual(len(self.s.snapshot()['matches']), 2)

    def test_live_denied_until_group_allowlisted(self):
        self.assertEqual(self.s.ingest(self.event)['accepted'], 0)
        self.s.set_config([DEMO_GROUPS[0]])
        changed = copy.deepcopy(self.event)
        changed['new_delivery'] = True
        r = self.s.ingest(changed)
        self.assertEqual(r['accepted'], 7)
        self.assertEqual(len(self.s.snapshot()['matches']), 1)

    def test_delete_removes_match_and_replay_cannot_revive(self):
        self.s.ingest(self.event, 'demo')
        self.s.ingest({'channel_id': 'DEMO-LOCAL', 'event': {'type': 'messages', 'event': 'delete'},
                       'messages': [{'id': 'demo-s1'}]}, 'demo')
        self.assertEqual(len(self.s.snapshot()['matches']), 1)
        replay = copy.deepcopy(self.event)
        replay['trace'] = 'late-replay'
        self.s.ingest(replay, 'demo')
        self.assertEqual(len(self.s.snapshot()['matches']), 1)

    def test_edit_changes_budget_and_removes_match(self):
        self.s.ingest(self.event, 'demo')
        self.s.ingest({'channel_id': 'DEMO-LOCAL', 'event': {'type': 'messages', 'event': 'patch'},
                       'messages': [{'id': 'demo-s1', 'text': {'body': '出售沙发 伦敦 £180'}}]}, 'demo')
        self.assertEqual(len(self.s.snapshot()['matches']), 1)

    def test_sparse_status_does_not_erase_text(self):
        self.s.ingest(self.event, 'demo')
        self.s.ingest({'channel_id': 'DEMO-LOCAL', 'event': {'type': 'messages', 'event': 'patch'},
                       'messages': [{'id': 'demo-s1', 'status': 'read'}]}, 'demo')
        self.assertEqual(len(self.s.snapshot()['matches']), 2)

    def test_demo_and_live_never_mix(self):
        self.s.set_config(DEMO_GROUPS)
        self.s.ingest(self.event, 'demo')
        self.s.ingest(self.event, 'whapi')
        matches = self.s.snapshot()['matches']
        self.assertEqual(len(matches), 4)
        self.assertTrue(all(c['offer']['origin'] == c['wanted']['origin'] for c in matches))

    def test_group_receipt_needs_viewer(self):
        self.s.set_config(DEMO_GROUPS)
        event = {'channel_id': 'real', 'event': {'type': 'statuses', 'event': 'put'}, 'statuses': [
            {'id': 'mid', 'recipient_id': DEMO_GROUPS[0], 'status': 'read', 'timestamp': 10},
            {'id': 'mid', 'recipient_id': DEMO_GROUPS[0], 'viewer_id': 'opaque@lid', 'status': 'delivered', 'timestamp': 20}]}
        self.s.ingest(event)
        self.assertEqual(len(self.s.snapshot()['delivery']), 1)

    def test_removing_group_from_allowlist_stops_matching(self):
        self.s.set_config(DEMO_GROUPS)
        self.s.ingest(self.event)
        self.assertEqual(len(self.s.snapshot()['matches']), 2)
        self.s.set_config([])
        self.assertEqual(self.s.snapshot()['matches'], [])

    def test_old_messages_do_not_trigger_matches(self):
        for m in self.event['messages']:
            m['timestamp'] = time.time() - 8*86400
        self.s.ingest(self.event, 'demo')
        self.assertEqual(self.s.snapshot()['matches'], [])

    def test_sqlite_survives_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            a = Store(Path(temp) / 'data.db')
            a.ingest(self.event, 'demo')
            a.db.close()
            b = Store(Path(temp) / 'data.db')
            self.assertEqual(len(b.snapshot()['matches']), 2)
            b.db.close()


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.app = App(Path(cls.temp.name), 0)
        cls.admin = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(cls.app))
        cls.app.port = cls.admin.server_port
        cls.ingress = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(cls.app, True))
        for server in (cls.admin, cls.ingress):
            threading.Thread(target=server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        for server in (cls.admin, cls.ingress):
            server.shutdown()
            server.server_close()
        cls.app.store.db.close()
        cls.temp.cleanup()

    def request(self, server, path, body=None, headers=None):
        req = urllib.request.Request(f'http://127.0.0.1:{server.server_port}{path}',
              data=json.dumps(body).encode() if body is not None else None,
              headers={'Content-Type': 'application/json', **(headers or {})})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_webhook_authentication_required(self):
        self.assertEqual(self.request(self.ingress, '/webhook', demo_events()[0])[0], 401)

    def test_tunnel_never_exposes_dashboard(self):
        for path in ('/', '/api/state', '/api/webhook-settings', '/app.js'):
            self.assertEqual(self.request(self.ingress, path)[0], 404)

    def test_webhook_persisted_before_ack(self):
        self.app.store.set_config(DEMO_GROUPS)
        code, body = self.request(self.ingress, '/webhook', demo_events()[0], {'X-Whapi-Poc-Secret': self.app.webhook_secret})
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)['accepted'], 9)
        self.assertEqual(self.app.store.snapshot()['counts']['whapi'], 9)

    def test_real_sending_disabled(self):
        self.assertEqual(self.request(self.admin, '/api/send', {}, {'X-Poc-Request': '1'})[0], 403)

    def test_cross_origin_rejected(self):
        self.assertEqual(self.request(self.admin, '/api/replay', {}, {'X-Poc-Request': '1', 'Origin': 'https://untrusted.example'})[0], 403)

    def test_host_rebinding_rejected(self):
        self.assertEqual(self.request(self.admin, '/api/state', headers={'Host': 'attacker.example'})[0], 404)

    def test_token_is_not_returned_or_persisted(self):
        token = 'synthetic-test-token-not-a-real-key'
        with patch.object(self.app, 'provider_get', side_effect=[{'status': {'text': 'AUTH'}}, {'groups': []}]):
            self.assertEqual(self.request(self.admin, '/api/connect', {'token': token}, {'X-Poc-Request': '1'})[0], 200)
        code, body = self.request(self.admin, '/api/state')
        self.assertNotIn(token.encode(), body)
        for path in Path(self.temp.name).glob('*'):
            if path.is_file():
                self.assertNotIn(token.encode(), path.read_bytes())

    def test_no_generic_provider_proxy(self):
        with self.assertRaises(ValueError):
            self.app.provider_get('/messages/text')

    def test_private_config_does_not_enable_sending(self):
        code, body = self.request(self.admin, '/api/private-config',
                                 {'chat_id': '15550000011@s.whatsapp.net', 'name': '测试联系人'},
                                 {'X-Poc-Request': '1'})
        self.assertEqual(code, 200)
        self.assertFalse(json.loads(body)['send_enabled'])
        self.assertEqual(self.app.store.config()['allowed_groups'], [])


class PrivateTests(unittest.TestCase):
    PEER = '15550000011@s.whatsapp.net'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = App(Path(self.temp.name), 0)
        self.app.store.set_channel('PRIVATE-TEST')
        self.app.store.set_private(self.PEER, '测试联系人')

    def tearDown(self):
        self.app.store.db.close()
        self.temp.cleanup()

    def message(self, **changes):
        data = {'id': 'private-1', 'chat_id': self.PEER, 'from': self.PEER.split('@')[0],
                'from_me': False, 'timestamp': int(time.time()), 'type': 'text',
                'text': {'body': '求购书桌，伦敦，预算£60。'}}
        return {**data, **changes}

    def ingest(self, messages):
        return self.app.store.ingest({'channel_id': 'PRIVATE-TEST',
                 'event': {'type': 'messages', 'event': 'post'}, 'messages': messages}, transport='poll')

    def test_only_explicit_phone_jids_are_normalized(self):
        self.assertEqual(private_id('15550000011@c.us'), self.PEER)
        self.assertEqual(private_id('+15550000011'), self.PEER)
        self.assertIsNone(private_id('55550000011@lid'))
        self.assertIsNone(private_id(DEMO_GROUPS[0]))

    def test_private_mode_disables_group_access(self):
        self.assertEqual(self.app.store.config()['allowed_groups'], [])
        self.assertEqual(self.app.store.config()['private_peer']['id'], self.PEER)

    def test_selecting_groups_disables_private_access(self):
        self.app.store.set_config(DEMO_GROUPS)
        self.assertIsNone(self.app.store.config()['private_peer'])
        self.assertEqual(self.ingest([self.message()])['accepted'], 0)

    def test_other_peer_and_own_messages_are_ignored(self):
        result = self.ingest([self.message(), self.message(id='other', chat_id='15550000022@s.whatsapp.net', **{'from': '15550000022'}),
                             self.message(id='self', from_me=True), self.message(id='group', chat_id=DEMO_GROUPS[0])])
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(result['ignored'], 3)

    def test_sender_must_agree_with_selected_private_chat(self):
        self.assertEqual(self.ingest([self.message(**{'from': '15550000022'})])['accepted'], 0)

    def test_private_messages_before_activation_are_not_imported(self):
        self.assertEqual(self.ingest([self.message(timestamp=int(time.time()) - 600)])['accepted'], 0)

    def test_same_person_cannot_create_self_match_across_jid_formats(self):
        self.ingest([self.message(text={'body': '出售书桌，伦敦，£40。'}),
                     self.message(id='private-2', chat_id='15550000011@c.us', **{'from': self.PEER})])
        snap = self.app.store.snapshot()
        self.assertEqual(snap['counts']['whapi'], 2)
        self.assertEqual(snap['matches'], [])

    def test_repeated_selection_does_not_reset_activation(self):
        armed = self.app.store.config()['private_peer']['armed_at']
        with patch('store.time.time', return_value=time.time() + 100):
            self.app.store.set_private(self.PEER, '测试联系人')
        self.assertEqual(self.app.store.config()['private_peer']['armed_at'], armed)

    def test_channel_change_revokes_private_access(self):
        self.app.store.set_channel('DIFFERENT-CHANNEL')
        self.assertIsNone(self.app.store.config()['private_peer'])

    def test_polling_reads_only_selected_peer_and_labels_it(self):
        with patch.object(self.app, 'provider_get', return_value={'messages': [self.message()]}) as getter:
            result = self.app.poll()
        self.assertEqual(result['accepted'], 1)
        self.assertIn('15550000011%40s.whatsapp.net', getter.call_args.args[0])
        self.assertEqual(self.app.store.snapshot()['messages'][0]['sender_name'], '测试联系人')

    def test_other_private_chat_path_is_denied(self):
        with self.assertRaises(ValueError):
            self.app.provider_get('/messages/list/15550000022%40s.whatsapp.net?count=100')


class PollingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = App(Path(self.temp.name), 0)
        self.app.store.set_channel('TEST-CHANNEL')
        self.app.store.set_config([DEMO_GROUPS[0]])

    def tearDown(self):
        self.app.store.db.close()
        self.temp.cleanup()

    def test_no_group_means_no_upstream_read(self):
        self.app.store.set_config([])
        with patch.object(self.app, 'provider_get') as upstream:
            with self.assertRaises(ValueError):
                self.app.poll()
            upstream.assert_not_called()

    def test_no_channel_means_no_upstream_read(self):
        self.app.store.db.execute("DELETE FROM config WHERE key='channel_id'")
        with patch.object(self.app, 'provider_get') as upstream:
            with self.assertRaises(ValueError):
                self.app.poll()
            upstream.assert_not_called()

    def test_polling_matches_without_webhook_or_send(self):
        messages = demo_events()[0]['messages']
        with patch.object(self.app, 'provider_get', return_value={'messages': messages}) as upstream:
            result = self.app.poll()
        self.assertEqual(result['calls'], 1)
        self.assertEqual(result['accepted'], 7)
        snap = self.app.store.snapshot()
        self.assertEqual(len(snap['matches']), 1)
        self.assertEqual(snap['receipts'][0]['result']['transport'], 'poll')
        self.assertTrue(upstream.call_args.args[0].startswith('/messages/list/'))

    def test_repeat_pull_deduplicates(self):
        with patch.object(self.app, 'provider_get', return_value={'messages': demo_events()[0]['messages']}):
            self.app.poll()
            self.app.poll()
        self.assertEqual(self.app.store.snapshot()['counts']['whapi'], 7)
        self.assertEqual(len(self.app.store.snapshot()['matches']), 1)

    def test_read_budget_is_hard_capped(self):
        with patch.object(self.app, 'provider_get', return_value={'messages': []}) as upstream:
            for i in range(20):
                self.app.poll()
            with self.assertRaises(ValueError):
                self.app.poll()
        self.assertEqual(upstream.call_count, 20)

    def test_forbidden_group_path_cannot_reach_provider(self):
        with self.assertRaises(ValueError):
            self.app.provider_get('/messages/list/not-allowed%40g.us?count=100')

    def test_reauthorizing_different_channel_clears_group_permissions(self):
        self.app.store.set_channel('ANOTHER-CHANNEL')
        self.assertEqual(self.app.store.config()['allowed_groups'], [])

    def test_pagination_consumes_explicit_budget(self):
        base = demo_events()[0]['messages'][0]
        messages = [{**base, 'id': f'p-{i}', 'text': {'body': 'ordinary conversation'}} for i in range(101)]
        with patch.object(self.app, 'provider_get', side_effect=[{'messages': messages[:100]}, {'messages': messages[100:]}]):
            result = self.app.poll()
        self.assertEqual(result['calls'], 2)
        self.assertEqual(result['accepted'], 101)
        self.assertEqual(result['pending_groups'], 0)

    def test_old_history_outside_armed_window_is_not_imported(self):
        old = demo_events()[0]['messages'][0]
        old['timestamp'] = int(time.time()) - 3600
        with patch.object(self.app, 'provider_get', return_value={'messages': [old]}):
            self.app.poll()
        self.assertEqual(self.app.store.snapshot()['counts']['whapi'], 0)

    def test_provider_error_does_not_reflect_credentials(self):
        self.app.token = 'synthetic-only-secret'
        error = urllib.error.HTTPError('https://gate.whapi.cloud/health', 403, 'Forbidden', {},
                                      io.BytesIO(b'{"error":"invalid token synthetic-only-secret"}'))
        with patch('urllib.request.build_opener') as build:
            build.return_value.open.side_effect = error
            with self.assertRaises(ValueError) as ctx:
                self.app.provider_get('/health?wakeup=false')
        self.assertNotIn(self.app.token, str(ctx.exception))
        self.assertIn('token-rejected', str(ctx.exception))


if __name__ == '__main__':
    unittest.main(verbosity=2)
