"""Bounded, single-peer rule replies. Claims are durable before network writes."""
import json
import math
import threading
import time


def reply_body(message):
    x = message['extracted']
    if x.get('intent') in ('wanted', 'offer') and x.get('item_label'):
        parts = ['求购' if x['intent'] == 'wanted' else '出售', x['item_label']]
        if x.get('city'):
            parts.append(x['city'])
        if x.get('price') is not None:
            parts.append(('预算 ' if x['intent'] == 'wanted' else '报价 ') +
                         f"{x['price']:g} {x.get('currency') or ''}")
        return ('我是 WhatsApp 自动化测试助手。已识别：' + ' / '.join(parts) +
                '。这条信息已记录在本机测试台，目前尚未完成买卖双方撮合。若识别有误，请发一条更正。')
    return '我是 WhatsApp 自动化测试助手，收到你的测试消息了。目前支持识别出售/求购的商品、城市和价格；可以按“求购书桌，伦敦，预算60英镑”的格式测试。'


class AutoReply:
    def __init__(self, app):
        self.app = app
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.state = {'active': False, 'reason': '尚未启用', 'attempts': 0, 'accepted': 0}
        self.app.store.db.execute('''CREATE TABLE IF NOT EXISTS auto_replies (
            inbound_key TEXT PRIMARY KEY, at REAL, body TEXT, status TEXT,
            message_id TEXT, detail TEXT)''')
        self.app.store.db.commit()

    def snapshot(self):
        with self.lock, self.app.store.lock:
            receipts = [dict(r) for r in self.app.store.db.execute(
                'SELECT at,body,status,message_id,detail FROM auto_replies ORDER BY at DESC LIMIT 20')]
            return {**self.state, 'receipts': receipts}

    def start(self, duration, max_messages):
        if type(duration) is not int or type(max_messages) is not int or (duration, max_messages) not in ((1800,10),(600,3),(3600,20)):
            raise ValueError('请选择预设的测试时限和条数')
        with self.lock:
            if self.state['active']:
                raise ValueError('自动回复已经运行；请先停止')
            cfg = self.app.store.config()
            peer = cfg.get('private_peer')
            if not peer or cfg.get('allowed_groups') or not self.app.token:
                raise ValueError('需先连接频道并仅启用一个指定私聊联系人')
            now = time.time()
            messages = [m for m in self.app.store.snapshot()['messages']
                        if m['origin']=='whapi' and m['chat_id']==peer['id'] and m['active']
                        and m['timestamp'] >= max(peer['armed_at'], now-3600)]
            latest = max(messages, key=lambda m:m['timestamp'], default=None)
            self.state = {'active':True, 'peer':peer['id'], 'name':peer['name'],
                          'channel':cfg['channel_id'], 'started_at':now, 'expires_at':now+duration,
                          'max_messages':max_messages, 'attempts':0, 'accepted':0,
                          'reason':'运行中', 'latest_key':latest['key'] if latest else None,
                          'baseline_keys':[m['key'] for m in self.app.store.snapshot()['messages']],
                          'last_poll':None, 'read_calls':0, 'read_limit':math.ceil(duration/15)+5}
            self.wake.clear()
            threading.Thread(target=self._run, args=(now,), daemon=True).start()
            return self.snapshot()

    def stop(self, reason='已手动停止'):
        with self.lock:
            self.state.update(active=False, reason=reason)
            self.wake.set()
        return self.snapshot()

    def tick(self):
        with self.lock:
            s = self.state
            if not s['active']:
                return
            cfg = self.app.store.config()
            if time.time() >= s['expires_at']:
                return self.stop('测试时间已到')
            if (cfg.get('private_peer') or {}).get('id') != s['peer'] or cfg.get('channel_id') != s['channel'] or cfg.get('allowed_groups'):
                return self.stop('白名单或频道发生变化')
            messages = sorted(self.app.store.snapshot()['messages'], key=lambda m:m['timestamp'])
            for m in messages:
                if not (m['origin']=='whapi' and m['channel']==s['channel'] and
                        m['chat_id']==s['peer'] and m['sender_id']==s['peer'] and m['active'] and
                        m['kind']=='text' and m['text'] and
                        ((m['timestamp'] >= math.floor(s['started_at']) and m['key'] not in s['baseline_keys'])
                         or m['key']==s['latest_key'])):
                    continue
                if s['attempts'] >= s['max_messages']:
                    return self.stop('回复条数上限已到')
                if time.time() >= s['expires_at']:
                    return self.stop('测试时间已到')
                body = reply_body(m)
                with self.app.store.lock, self.app.store.db:
                    claimed = self.app.store.db.execute('INSERT OR IGNORE INTO auto_replies VALUES (?,?,?,?,?,?)',
                        (m['key'],time.time(),body,'attempting',None,'发送前已持久化；未知结果不自动重试')).rowcount
                if not claimed:
                    continue
                s['attempts'] += 1
                try:
                    result = self.app.provider_send(s['peer'],body)
                    msg = result.get('message',{})
                    accepted = result.get('sent') is True and isinstance(msg.get('id'),str)
                    status = 'accepted' if accepted else 'uncertain'
                    detail = '供应商已受理；不代表对方送达/已读' if accepted else '结果未确认，停止且不重试'
                    with self.app.store.lock, self.app.store.db:
                        self.app.store.db.execute('UPDATE auto_replies SET status=?,message_id=?,detail=? WHERE inbound_key=?',
                                                 (status,msg.get('id'),detail,m['key']))
                    if not accepted:
                        return self.stop('发送结果未确认，已停止')
                    s['accepted'] += 1
                    if s['attempts'] >= s['max_messages']:
                        return self.stop('回复条数上限已到')
                except Exception:
                    with self.app.store.lock, self.app.store.db:
                        self.app.store.db.execute("UPDATE auto_replies SET status='uncertain',detail=? WHERE inbound_key=?",
                            ('发送失败或结果未知；不自动重试',m['key']))
                    return self.stop('发送失败或结果未知，已停止')

    def _run(self, run_id):
        try:
            while self.state['active'] and self.state['started_at']==run_id:
                self.tick()
                if not self.state['active'] or self.state['started_at']!=run_id:
                    break
                self.app.poll(automatic=True)
                with self.lock:
                    self.state['last_poll'] = time.time()
                self.tick()
                self.wake.wait(15)
        except Exception:
            if self.state.get('started_at')==run_id:
                self.stop('读取失败或预算耗尽，已停止；可检查后重新启用')
