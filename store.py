import hashlib
import json
import math
import re
import sqlite3
import threading
import time
from matcher import extract, match_pair

DEMO_GROUPS = ['120363000000001@g.us', '120363000000002@g.us']


def private_id(value):
    """Only explicit phone-based direct chats; never infer a phone from an opaque LID."""
    if not isinstance(value, str):
        return None
    m = re.fullmatch(r'\+?(\d{7,15})(?:@(s\.whatsapp\.net|c\.us))?', value.strip())
    return m.group(1) + '@s.whatsapp.net' if m else None


def allowed_chats(config):
    ids = list(config.get('allowed_groups', []))
    peer = config.get('private_peer')
    if isinstance(peer, dict) and peer.get('id'):
        ids.append(peer['id'])
    return ids


class Store:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
          PRAGMA journal_mode=WAL;
          CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS receipts (origin TEXT, digest TEXT, at REAL, channel TEXT,
             event TEXT, result TEXT, PRIMARY KEY(origin,digest));
          CREATE TABLE IF NOT EXISTS messages (key TEXT PRIMARY KEY, origin TEXT, channel TEXT,
             message_id TEXT, chat_id TEXT, sender_id TEXT, sender_name TEXT, timestamp REAL,
             received_at REAL, text TEXT, kind TEXT, media TEXT, extracted TEXT,
             active INTEGER, deleted INTEGER DEFAULT 0);
          CREATE TABLE IF NOT EXISTS delivery (key TEXT PRIMARY KEY, origin TEXT, channel TEXT,
             message_id TEXT, chat_id TEXT, viewer_id TEXT, status TEXT, at REAL);
          CREATE TABLE IF NOT EXISTS signals (key TEXT PRIMARY KEY, value TEXT, at REAL);
        ''')
        self.db.commit()

    def config(self):
        with self.lock:
            return {r['key']: json.loads(r['value']) for r in self.db.execute('SELECT * FROM config')}

    def set_config(self, groups):
        if not isinstance(groups, list) or len(groups) > 100 or any(not isinstance(g, str) or not g.endswith('@g.us') or len(g) > 100 for g in groups):
            raise ValueError('仅允许明确的群ID（@g.us），最多100个')
        with self.lock, self.db:
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('private_peer', 'null'))
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('allowed_groups', json.dumps(sorted(set(groups)))))
            since = self.config().get('poll_since', {})
            since = {g: since.get(g, int(time.time())) for g in groups}
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('poll_since', json.dumps(since)))

    def set_private(self, chat_id, name):
        peer = private_id(chat_id)
        if not peer:
            raise ValueError('仅允许一个明确的手机号私聊ID；不自动解析或扩大LID权限')
        if not isinstance(name, str) or not name.strip() or len(name) > 80:
            raise ValueError('请填写已确认的联系人名称')
        with self.lock, self.db:
            old = self.config().get('private_peer')
            since = old['armed_at'] if isinstance(old, dict) and old.get('id') == peer else int(time.time())
            selected = {'id': peer, 'name': name.strip(), 'armed_at': since}
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('private_peer', json.dumps(selected, ensure_ascii=False)))
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('allowed_groups', '[]'))
            old_since = self.config().get('poll_since', {})
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('poll_since', json.dumps({peer: max(since, old_since.get(peer, since))})))
        return selected

    def set_channel(self, channel):
        if not isinstance(channel, str) or not 2 <= len(channel) <= 100 or any(c not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_' for c in channel):
            raise ValueError('请输入 Whapi 控制台显示的 Channel ID')
        with self.lock, self.db:
            old = self.config().get('channel_id')
            if old != channel:
                self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('allowed_groups', '[]'))
                self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('poll_since', '{}'))
                self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('private_peer', 'null'))
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('channel_id', json.dumps(channel)))

    def checkpoint(self, group, timestamp):
        with self.lock, self.db:
            cfg = self.config()
            if group not in allowed_chats(cfg):
                return
            since = cfg.get('poll_since', {})
            since[group] = timestamp
            self.db.execute('INSERT OR REPLACE INTO config VALUES (?,?)', ('poll_since', json.dumps(since)))

    @staticmethod
    def key(origin, channel, mid):
        return hashlib.sha256(f'{origin}\0{channel}\0{mid}'.encode()).hexdigest()[:32]

    def ingest(self, payload, origin='whapi', transport=None):
        if not isinstance(payload, dict):
            raise ValueError('Webhook 必须是 JSON 对象')
        channel = payload.get('channel_id')
        if not isinstance(channel, str) or not channel or len(channel) > 100:
            raise ValueError('缺少有效 channel_id')
        ev = payload.get('event', {})
        if not isinstance(ev, dict):
            raise ValueError('无效 event')
        typ, action = ev.get('type', ''), ev.get('event', '')
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        result = {'accepted': 0, 'duplicate': 0, 'ignored': 0, 'updated': 0}
        if transport:
            result['transport'] = transport
        with self.lock, self.db:
            if self.db.execute('SELECT 1 FROM receipts WHERE origin=? AND digest=?', (origin, digest)).fetchone():
                return {**result, 'duplicate': 1, 'event_duplicate': True}
            cfg = self.config()
            allow = DEMO_GROUPS if origin == 'demo' else allowed_chats(cfg)
            peer = cfg.get('private_peer') if origin != 'demo' else None
            if typ == 'messages':
                messages = payload.get('messages', [])
                if not isinstance(messages, list) or len(messages) > 200:
                    raise ValueError('messages 应为最多200条的数组')
                for m in messages:
                    if not isinstance(m, dict) or not isinstance(m.get('id'), str):
                        result['ignored'] += 1
                        continue
                    mid = m['id']
                    if len(mid) > 200:
                        result['ignored'] += 1
                        continue
                    key = self.key(origin, channel, mid)
                    old = self.db.execute('SELECT * FROM messages WHERE key=?', (key,)).fetchone()
                    if action == 'delete':
                        # Tombstones prevent old callback replays from reviving a withdrawn post.
                        if old:
                            self.db.execute('UPDATE messages SET deleted=1,active=0 WHERE key=?', (key,))
                        else:
                            self.db.execute('INSERT INTO messages(key,origin,channel,message_id,deleted,active) VALUES (?,?,?,?,1,0)', (key, origin, channel, mid))
                        result['updated'] += 1
                        continue
                    if old and old['deleted']:
                        result['ignored'] += 1
                        continue
                    if action not in ('post', 'put', 'patch'):
                        result['ignored'] += 1
                        continue
                    # Sparse changes may omit chat/sender/from_me. Only known messages can be patched.
                    chat = m.get('chat_id') or (old['chat_id'] if old else None)
                    chat = private_id(chat) or chat
                    if chat not in allow or m.get('from_me') is True:
                        result['ignored'] += 1
                        continue
                    if old and action in ('post', 'put'):
                        result['duplicate'] += 1
                        continue
                    if not old and m.get('from_me') is not False:
                        result['ignored'] += 1
                        continue
                    text_obj = m.get('text', {})
                    if not isinstance(text_obj, dict):
                        text_obj = {}
                    body = text_obj.get('body')
                    # Whapi action.edit carries the replacement body in action.body.
                    act = m.get('action', {})
                    if isinstance(act, dict) and act.get('type') == 'edit':
                        target = act.get('target')
                        if target and target != mid:
                            key = self.key(origin, channel, target)
                            old = self.db.execute('SELECT * FROM messages WHERE key=?', (key,)).fetchone()
                            if not old or old['deleted']:
                                result['ignored'] += 1
                                continue
                            chat, mid = old['chat_id'], old['message_id']
                            if chat not in allow:
                                result['ignored'] += 1
                                continue
                        body = act.get('body')
                    media = {}
                    for media_kind in ('image', 'video', 'voice', 'audio', 'document'):
                        obj = m.get(media_kind)
                        if isinstance(obj, dict):
                            media = {k: obj[k] for k in ('id', 'mime_type', 'file_size', 'file_name', 'caption') if k in obj}
                            if body is None:
                                body = obj.get('caption', '')
                    if body is None and old:
                        # Status/metadata patch must not erase the original text.
                        result['ignored'] += 1
                        continue
                    body = body if isinstance(body, str) else ''
                    body = body[:12000]
                    sender = m.get('from') or (old['sender_id'] if old else '')
                    if not isinstance(sender, str) or not sender:
                        result['ignored'] += 1
                        continue
                    if isinstance(peer, dict) and chat == peer['id'] and private_id(sender) != peer['id']:
                        result['ignored'] += 1
                        continue
                    if isinstance(peer, dict) and chat == peer['id']:
                        sender = peer['id']
                    ts = m.get('timestamp', old['timestamp'] if old else time.time())
                    try:
                        ts = float(ts)
                    except (ValueError, TypeError):
                        result['ignored'] += 1
                        continue
                    if not math.isfinite(ts) or (isinstance(peer, dict) and chat == peer['id'] and ts < peer['armed_at']):
                        result['ignored'] += 1
                        continue
                    parsed = extract(body)
                    active = parsed['intent'] in ('offer', 'wanted') and bool(parsed.get('item'))
                    self.db.execute('''INSERT OR REPLACE INTO messages VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)''',
                        (key, origin, channel, mid, chat, sender[:150], str(m.get('from_name') or (old['sender_name'] if old else '') or (peer['name'] if isinstance(peer, dict) and chat == peer['id'] else ''))[:150],
                         ts, time.time(), body, str(m.get('type', 'text'))[:40], json.dumps(media, ensure_ascii=False),
                         json.dumps(parsed, ensure_ascii=False), int(active)))
                    result['updated' if old else 'accepted'] += 1
            elif typ == 'statuses':
                statuses = payload.get('statuses', [])
                if not isinstance(statuses, list) or len(statuses) > 500:
                    raise ValueError('statuses 数组无效')
                for s in statuses:
                    if not isinstance(s, dict) or not isinstance(s.get('id'), str):
                        continue
                    chat, viewer = s.get('recipient_id', ''), s.get('viewer_id', '')
                    chat = private_id(chat) or chat
                    if chat not in allow or (chat.endswith('@g.us') and not viewer):
                        result['ignored'] += 1
                        continue
                    k = self.key(origin, channel, s['id'] + ':' + str(viewer))
                    try:
                        at = float(s.get('timestamp', time.time()))
                    except (TypeError, ValueError):
                        continue
                    old_s = self.db.execute('SELECT at FROM delivery WHERE key=?', (k,)).fetchone()
                    if old_s and old_s['at'] > at:
                        continue
                    self.db.execute('INSERT OR REPLACE INTO delivery VALUES (?,?,?,?,?,?,?,?)',
                                    (k, origin, channel, s['id'], chat, str(viewer), str(s.get('status', 'unknown'))[:30], at))
                    result['accepted'] += 1
            elif typ in ('channel', 'channels'):
                health = payload.get('health', {})
                status = health.get('status', {}) if isinstance(health, dict) else {}
                self.db.execute('INSERT OR REPLACE INTO signals VALUES (?,?,?)',
                                ('channel:' + origin, json.dumps({'channel_id': channel, 'status': status.get('text', 'unknown')}, ensure_ascii=False), time.time()))
                result['accepted'] += 1
            else:
                result['ignored'] += 1
            self.db.execute('INSERT INTO receipts VALUES (?,?,?,?,?,?)',
                            (origin, digest, time.time(), channel, str(typ)[:40] + '.' + str(action)[:20], json.dumps(result)))
        return result

    def snapshot(self):
        with self.lock:
            records = []
            for row in self.db.execute('SELECT * FROM messages WHERE chat_id IS NOT NULL ORDER BY received_at DESC LIMIT 500'):
                r = dict(row)
                r['extracted'] = json.loads(r['extracted'])
                r['media'] = json.loads(r['media'])
                r['active'] = bool(r['active']) and not r['deleted']
                records.append(r)
            # Recent records only; ancient or future timestamps cannot trigger new candidates.
            allowed = allowed_chats(self.config())
            fresh = [r for r in records if r['active']
                     and (r['origin'] == 'demo' or r['chat_id'] in allowed)
                     and time.time() - 7*86400 <= r['timestamp'] <= time.time() + 300]
            candidates = []
            for i, a in enumerate(fresh):
                for b in fresh[i+1:]:
                    if (a['origin'], a['channel'], a['chat_id']) != (b['origin'], b['channel'], b['chat_id']):
                        continue
                    found = match_pair(a, b)
                    if found:
                        candidates.append({**found, 'origin': a['origin'], 'chat_id': a['chat_id'],
                                           'offer': next(r for r in (a, b) if r['key'] == found['offer_key']),
                                           'wanted': next(r for r in (a, b) if r['key'] == found['wanted_key'])})
            receipts = [dict(r) for r in self.db.execute('SELECT origin,at,event,result FROM receipts ORDER BY at DESC LIMIT 25')]
            for r in receipts:
                r['result'] = json.loads(r['result'])
            return {'messages': records, 'matches': candidates, 'receipts': receipts,
                    'delivery': [dict(r) for r in self.db.execute('SELECT * FROM delivery ORDER BY at DESC LIMIT 30')],
                    'counts': {'demo': sum(r['origin'] == 'demo' for r in records),
                               'whapi': sum(r['origin'] == 'whapi' for r in records),
                               'offers': sum(r['extracted']['intent'] == 'offer' and r['active'] for r in records),
                               'wanted': sum(r['extracted']['intent'] == 'wanted' and r['active'] for r in records)},
                    'config': self.config(), 'signals': [dict(r) for r in self.db.execute('SELECT * FROM signals')]}
