import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from server import App


class AutoTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.app = App(Path(self.tmp.name), 8797)
        self.app.token = 'test-token-never-real'
        self.app.store.set_channel('TEST-CHANNEL')
        self.peer = '15550000011@s.whatsapp.net'
        self.app.store.set_private(self.peer, '测试联系人')
        self.add('one')

    def tearDown(self):
        self.app.auto.stop()
        self.app.store.db.close()
        self.tmp.cleanup()

    def add(self, mid, **kw):
        m = {'id':mid,'chat_id':self.peer,'from':self.peer,'from_me':False,
             'timestamp':int(time.time()),'type':'text','text':{'body':'求购书桌 伦敦 预算60英镑'}}
        m.update(kw)
        self.app.store.ingest({'channel_id':'TEST-CHANNEL','messages':[m],
                              'event':{'type':'messages','event':'post'}},'whapi')

    def start(self, duration=600, limit=3):
        with patch('threading.Thread.start'):
            self.app.auto.start(duration,limit)

    def send(self):
        return patch.object(self.app,'provider_send',return_value={'sent':True,'message':{'id':'out-id'}})

    def test_latest_real_message_and_dedup(self):
        self.start()
        with self.send() as send:
            self.app.auto.tick();self.app.auto.tick()
        self.assertEqual(send.call_count,1)
        self.assertEqual(send.call_args.args[0],self.peer)
        self.assertIn('预算 60 GBP',send.call_args.args[1])
        self.assertEqual(self.app.auto.snapshot()['accepted'],1)

    def test_expiry_no_send(self):
        self.start();self.app.auto.state['expires_at']=time.time()-1
        with self.send() as send:self.app.auto.tick()
        send.assert_not_called();self.assertFalse(self.app.auto.state['active'])

    def test_stop_no_send(self):
        self.start();self.app.auto.stop()
        with self.send() as send:self.app.auto.tick()
        send.assert_not_called()

    def test_limit(self):
        self.start()
        with self.send() as send:
            for i in range(5):
                self.add(str(i));self.app.auto.tick()
        self.assertEqual(send.call_count,3)
        self.assertFalse(self.app.auto.state['active'])

    def test_uncertain_stops_no_retry_even_reenable(self):
        self.start()
        with patch.object(self.app,'provider_send',side_effect=TimeoutError) as send:
            self.app.auto.tick();self.app.auto.tick()
            self.start();self.app.auto.tick()
        self.assertEqual(send.call_count,1)
        self.assertEqual(self.app.auto.snapshot()['receipts'][0]['status'],'uncertain')

    def test_old_and_self_and_other_ignored(self):
        self.add('old',timestamp=int(time.time())-7200)
        self.add('self',from_me=True)
        self.add('other',chat_id='15550000022@s.whatsapp.net')
        self.start()
        with self.send() as send:self.app.auto.tick()
        self.assertEqual(send.call_count,1)

    def test_scope_change_stops(self):
        self.start();self.app.store.set_config(['120363000000001@g.us'])
        with self.send() as send:self.app.auto.tick()
        send.assert_not_called();self.assertFalse(self.app.auto.state['active'])

    def test_reenable_does_not_duplicate(self):
        self.start()
        with self.send() as send:
            self.app.auto.tick();self.app.auto.stop();self.start();self.app.auto.tick()
        self.assertEqual(send.call_count,1)

    def test_invalid_budget(self):
        with self.assertRaises(ValueError):self.app.auto.start(999999,9999)

    def test_unconfirmed_response_stops(self):
        self.start()
        with patch.object(self.app,'provider_send',return_value={'sent':False}):self.app.auto.tick()
        self.assertFalse(self.app.auto.state['active'])

    def test_provider_cannot_target_other_peer(self):
        with self.assertRaises(ValueError):self.app.provider_send('15550000022@s.whatsapp.net','text')

    def test_restart_never_resumes(self):
        self.start()
        from autoreply import AutoReply
        self.assertFalse(AutoReply(self.app).state['active'])


if __name__=='__main__':unittest.main()
