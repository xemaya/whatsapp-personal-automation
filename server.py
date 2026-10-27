"""Two local listeners: dashboard/admin and a strictly webhook-only tunnel target."""
import argparse
import hmac
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fixtures import demo_events
from store import Store, private_id, allowed_chats
from autoreply import AutoReply

ROOT = Path(__file__).resolve().parent
MAX_BODY = 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class App:
    def __init__(self, runtime, port):
        runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(runtime, 0o700)
        self.store = Store(runtime / 'poc.sqlite3')
        self.port = port
        self.token = None
        self.token_lock = threading.Lock()
        self.provider_summary = {'status': '尚未连接', 'groups': []}
        keyfile = runtime / 'webhook-secret'
        if not keyfile.exists():
            fd = os.open(keyfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w') as f:
                f.write(secrets.token_urlsafe(36))
        os.chmod(keyfile, 0o600)
        self.webhook_secret = keyfile.read_text().strip()
        self.public_url = ''
        self.poll_lock = threading.Lock()
        self.poll_requests = 0
        self.poll_budget = 20
        self.poll_windows = {}
        self.auto = AutoReply(self)

    def provider_send(self, peer, body):
        cfg = self.store.config()
        if peer != (cfg.get('private_peer') or {}).get('id') or cfg.get('allowed_groups'):
            raise ValueError('发送目标必须是当前唯一私聊白名单')
        with self.token_lock:
            token = self.token
        if not token:
            raise ValueError('未连接频道')
        request = urllib.request.Request('https://gate.whapi.cloud/messages/text',
            data=json.dumps({'to':peer.split('@')[0], 'body':body, 'no_link_preview':True},ensure_ascii=False).encode(),
            headers={'Authorization':'Bearer '+token,'Content-Type':'application/json',
                     'Accept':'application/json','User-Agent':'WhapiGroupPOC/0.1'},method='POST')
        try:
            with urllib.request.build_opener(NoRedirect).open(request,timeout=20) as r:
                raw = r.read(MAX_BODY+1)
                if len(raw)>MAX_BODY:
                    raise ValueError('响应过大')
                return json.loads(raw)
        except Exception:
            raise ValueError('Whapi发送失败或结果未知；不自动重试，未记录敏感响应') from None

    def provider_get(self, path):
        # Never a generic proxy. Message reads are restricted to explicitly selected conversations.
        parts = urllib.parse.urlsplit(path)
        group_path = parts.path.removeprefix('/messages/list/')
        allowed_message_path = (parts.path.startswith('/messages/list/')
                                and urllib.parse.unquote(group_path) in allowed_chats(self.store.config())
                                and not parts.scheme and not parts.netloc and not parts.fragment)
        if path not in ('/health?wakeup=false', '/groups?count=100&offset=0') and not allowed_message_path:
            raise ValueError('只允许健康、群列表及明确白名单对话的新消息读取')
        with self.token_lock:
            token = self.token
        if not token:
            raise ValueError('请先在本机填写 Whapi API Token')
        request = urllib.request.Request('https://gate.whapi.cloud' + path,
                                         headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json',
                                                  'User-Agent': 'WhapiGroupPOC/0.1'})
        opener = urllib.request.build_opener(NoRedirect)
        try:
            with opener.open(request, timeout=20) as r:
                body = r.read(MAX_BODY + 1)
                if len(body) > MAX_BODY:
                    raise ValueError('供应商响应过大')
                return json.loads(body)
        except urllib.error.HTTPError as e:
            # Classify using fixed labels only; never expose the response body or a reflected token.
            body = e.read(MAX_BODY).decode('utf-8', errors='replace').lower()
            category = 'provider-rejected'
            if 'cloudflare' in body or 'cf-chl' in body or 'just a moment' in body:
                category = 'edge-challenge'
            elif 'token' in body and any(v in body for v in ('invalid', 'incorrect', 'unauthorized')):
                category = 'token-rejected'
            elif any(v in body for v in ('subscription', 'trial', 'payment', 'expired', 'limit exceeded')):
                category = 'subscription-or-quota'
            route = 'health' if path.startswith('/health') else 'messages' if allowed_message_path else 'groups'
            raise ValueError(f'Whapi {route} HTTP {e.code} ({category})；未记录响应正文') from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            raise ValueError('Whapi 连接失败或响应无效；未记录 Token/响应正文') from None

    def connect(self, token, channel_id=None):
        if not isinstance(token, str) or not 10 <= len(token) <= 1000 or any(c.isspace() for c in token):
            raise ValueError('Token 格式无效')
        with self.token_lock:
            self.token = token
        try:
            health = self.provider_get('/health?wakeup=false')
            status = health.get('status', health.get('health', {}).get('status', {}))
            if isinstance(status, dict):
                status = status.get('text', 'unknown')
            self.provider_summary = {'status': str(status)[:80], 'groups': [], 'token_loaded': True}
            data = self.provider_get('/groups?count=100&offset=0')
            groups = data.get('groups', [])
            if not isinstance(groups, list):
                raise ValueError('群列表响应格式无效')
            self.provider_summary['groups'] = [
                {'id': g['id'], 'name': str(g.get('name', g.get('subject', g['id'])))[:150]}
                for g in groups if isinstance(g, dict) and isinstance(g.get('id'), str) and g['id'].endswith('@g.us')]
            self.provider_summary['pagination_note'] = '仅读取前100个群；本 PoC 试点请选择少量群'
            if channel_id:
                self.store.set_channel(channel_id)
        except ValueError:
            with self.token_lock:
                self.token = None
            self.provider_summary = {'status': '连接未完成', 'groups': []}
            raise
        return self.provider_summary

    def poll(self, automatic=False):
        if not self.poll_lock.acquire(blocking=False):
            raise ValueError('已有一次群消息读取在进行')
        try:
            cfg = self.store.config()
            groups, channel = allowed_chats(cfg), cfg.get('channel_id')
            peer = cfg.get('private_peer')
            if not channel:
                raise ValueError('请先填写 Channel ID 并连接频道')
            if not groups or len(groups) > 3:
                raise ValueError('本次试点请选择1至3个测试群，或一个明确的私聊联系人')
            totals = {'accepted': 0, 'duplicate': 0, 'ignored': 0, 'updated': 0}
            calls = 0
            for group in groups:
                window = self.poll_windows.setdefault(group, {
                    'from': max(0, int(cfg.get('poll_since', {}).get(group, time.time())) - 2),
                    'to': int(time.time()), 'offset': 0})
                for page in range(5):
                    if automatic:
                        s = self.auto.state
                        if not s['active'] or time.time() >= s['expires_at'] or group != s['peer'] or s['read_calls'] >= s['read_limit']:
                            raise ValueError('自动读取的范围、时限或额度不满足')
                        s['read_calls'] += 1
                    elif self.poll_requests >= self.poll_budget:
                        raise ValueError('本次进程的20次消息API读取预算已用完；未自动扩大预算')
                    query = urllib.parse.urlencode({'count': 100, 'offset': window['offset'],
                             'time_from': window['from'], 'time_to': window['to'], 'sort': 'asc'})
                    path = '/messages/list/' + urllib.parse.quote(group, safe='') + '?' + query
                    if not automatic:
                        self.poll_requests += 1
                    calls += 1
                    response = self.provider_get(path)
                    messages = response.get('messages')
                    if not isinstance(messages, list) or len(messages) > 100:
                        raise ValueError('Whapi 消息响应格式异常；游标未推进')
                    filtered = []
                    for msg in messages:
                        if not isinstance(msg, dict):
                            continue
                        chat = private_id(msg.get('chat_id')) or msg.get('chat_id')
                        if chat != group:
                            continue
                        try:
                            ts = float(msg.get('timestamp'))
                        except (TypeError, ValueError):
                            continue
                        lower = max(window['from'], peer['armed_at']) if isinstance(peer, dict) and group == peer['id'] else window['from']
                        if lower <= ts <= window['to']:
                            filtered.append(msg)
                    result = self.store.ingest({'channel_id': channel, 'messages': filtered,
                                  'event': {'type': 'messages', 'event': 'post'},
                                  'poll_window': dict(window)}, 'whapi', transport='poll')
                    for key in totals:
                        totals[key] += result[key]
                    window['offset'] += len(messages)
                    if len(messages) < 100:
                        self.store.checkpoint(group, window['to'])
                        self.poll_windows.pop(group, None)
                        break
            return {'ok': True, 'transport': 'poll', 'calls': calls,
                    'remaining_reads': self.poll_budget - self.poll_requests,
                    'pending_groups': len(self.poll_windows), **totals,
                    'notice': '实际 Whapi API 读取；不代表 Webhook 已接通；仅读选定对话启用后的新消息'}
        finally:
            self.poll_lock.release()

    def settings(self):
        events = [{'type': t, 'method': method} for t, method in (
            ('messages', 'post'), ('messages', 'put'), ('messages', 'patch'), ('messages', 'delete'),
            ('statuses', 'put'), ('statuses', 'post'), ('channel', 'post'))]
        return {'webhooks': [{'url': (self.public_url + '/webhook') if self.public_url else 'https://YOUR-TUNNEL/webhook',
                              'mode': 'body', 'headers': {'X-Whapi-Poc-Secret': self.webhook_secret},
                              'events': events, 'webhook_max_age_seconds': 172800}],
                'callback_persist': True}


def handler_for(app, ingress=False):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'WhapiLocalPOC'

        def log_message(self, *args):
            pass  # No request paths, bodies, phone numbers, tokens or callback secrets in logs.

        def respond(self, code, data, content_type='application/json; charset=utf-8'):
            raw = json.dumps(data, ensure_ascii=False).encode() if isinstance(data, (dict, list)) else data
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
            self.end_headers()
            self.wfile.write(raw)

        def local_allowed(self):
            host = self.headers.get('Host', '')
            return host in (f'127.0.0.1:{app.port}', f'localhost:{app.port}')

        def body(self):
            if self.headers.get('Content-Type', '').split(';')[0] != 'application/json':
                raise ValueError('Content-Type 必须为 application/json')
            try:
                length = int(self.headers.get('Content-Length', '-1'))
            except ValueError:
                raise ValueError('无效 Content-Length') from None
            if not 0 <= length <= MAX_BODY:
                raise ValueError('请求大小超限或缺少 Content-Length')
            return json.loads(self.rfile.read(length))

        def do_GET(self):
            if ingress or not self.local_allowed():
                return self.respond(404, {'error': 'not found'})
            path = urllib.parse.urlsplit(self.path).path
            if path in ('/', '/app.js', '/style.css'):
                file = ROOT / 'static' / ({'/': 'index.html', '/app.js': 'app.js', '/style.css': 'style.css'}[path])
                types = {'.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css'}
                return self.respond(200, file.read_bytes(), types[file.suffix] + '; charset=utf-8')
            if path == '/api/state':
                data = app.store.snapshot()
                data.update({'provider': app.provider_summary, 'send_enabled': False, 'model': '本地规则 / 无模型调用',
                             'ingress_port': self.server.server_address[1] + 1,
                             'public_url': app.public_url, 'remaining_reads': app.poll_budget - app.poll_requests,
                             'auto_reply': app.auto.snapshot()})
                return self.respond(200, data)
            if path == '/api/webhook-settings':
                return self.respond(200, {'settings': app.settings(), 'notice': '仅供本机复制，不会自动 PATCH Whapi。更新供应商配置前需合并原配置，避免默认值重置。'})
            return self.respond(404, {'error': 'not found'})

        def do_POST(self):
            if ingress:
                if self.path != '/webhook':
                    return self.respond(404, {'error': 'not found'})
                secret = self.headers.get('X-Whapi-Poc-Secret', '')
                if not hmac.compare_digest(secret, app.webhook_secret):
                    return self.respond(401, {'error': 'unauthorized'})
                try:
                    result = app.store.ingest(self.body(), 'whapi', transport='webhook')
                    return self.respond(200, {'ok': True, **result})
                except (ValueError, TypeError, KeyError):
                    return self.respond(400, {'error': 'invalid webhook payload'})
                except Exception:
                    return self.respond(503, {'error': 'persist failed; retry required'})
            if not self.local_allowed():
                return self.respond(403, {'error': 'local access only'})
            if self.headers.get('X-Poc-Request') != '1':
                return self.respond(403, {'error': 'missing local request header'})
            origin = self.headers.get('Origin')
            if origin and origin not in (f'http://127.0.0.1:{app.port}', f'http://localhost:{app.port}'):
                return self.respond(403, {'error': 'origin rejected'})
            try:
                data = self.body()
                if not isinstance(data, dict):
                    raise ValueError('请求必须为对象')
                if self.path == '/api/replay':
                    return self.respond(200, {'results': [app.store.ingest(e, 'demo') for e in demo_events()]})
                if self.path == '/api/config':
                    app.auto.stop('群白名单更改，自动回复已停止')
                    if not app.poll_lock.acquire(blocking=False):
                        raise ValueError('读取进行中，完成后再更改白名单')
                    try:
                        app.store.set_config(data.get('allowed_groups', []))
                        app.poll_windows.clear()
                    finally:
                        app.poll_lock.release()
                    return self.respond(200, {'ok': True})
                if self.path == '/api/private-config':
                    app.auto.stop('私聊白名单更改，自动回复已停止')
                    if not app.poll_lock.acquire(blocking=False):
                        raise ValueError('读取进行中，完成后再更改白名单')
                    try:
                        selected = app.store.set_private(data.get('chat_id'), data.get('name'))
                        app.poll_windows.clear()
                    finally:
                        app.poll_lock.release()
                    return self.respond(200, {'ok': True, 'private_peer': selected, 'send_enabled': False})
                if self.path == '/api/connect':
                    app.auto.stop('重新连接频道，自动回复已停止')
                    return self.respond(200, {'provider': app.connect(data.get('token'), data.get('channel_id'))})
                if self.path == '/api/poll':
                    return self.respond(200, app.poll())
                if self.path == '/api/disconnect':
                    app.auto.stop('已清除Token，自动回复停止')
                    with app.token_lock:
                        app.token = None
                    app.provider_summary = {'status': '已清除本机 Token', 'groups': []}
                    return self.respond(200, {'ok': True})
                if self.path == '/api/public-url':
                    u = urllib.parse.urlsplit(data.get('url', ''))
                    if u.scheme != 'https' or not u.hostname or u.username or u.password or u.path not in ('', '/') or u.query or u.fragment:
                        raise ValueError('请输入不带路径或参数的 HTTPS 隧道域名')
                    app.public_url = 'https://' + u.netloc
                    return self.respond(200, {'ok': True})
                if self.path == '/api/send':
                    return self.respond(403, {'error': '真实发送硬关闭；本 PoC 无发送接口'})
                if self.path == '/api/auto/start':
                    return self.respond(200, app.auto.start(data.get('duration'), data.get('max_messages')))
                if self.path == '/api/auto/stop':
                    return self.respond(200, app.auto.stop())
                return self.respond(404, {'error': 'not found'})
            except (ValueError, KeyError, TypeError) as e:
                return self.respond(400, {'error': str(e)[:180]})
            except Exception:
                return self.respond(500, {'error': '本地操作失败，未记录敏感请求正文'})

        def do_PUT(self):
            return self.respond(405, {'error': 'method not allowed; configure Body-mode webhooks'})

        do_PATCH = do_PUT
        do_DELETE = do_PUT
        do_OPTIONS = do_PUT
    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8797)
    parser.add_argument('--ingress-port', type=int, default=8798)
    parser.add_argument('--runtime', type=Path, default=ROOT / 'runtime')
    args = parser.parse_args()
    app = App(args.runtime, args.port)
    ingress = ThreadingHTTPServer(('127.0.0.1', args.ingress_port), handler_for(app, True))
    threading.Thread(target=ingress.serve_forever, daemon=True).start()
    dashboard = ThreadingHTTPServer(('127.0.0.1', args.port), handler_for(app))
    print(f'Dashboard: http://127.0.0.1:{args.port}  | webhook-only port: {args.ingress_port}', flush=True)
    try:
        dashboard.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.auto.stop('服务停止')
        ingress.shutdown()
        dashboard.server_close()
        app.store.db.close()


if __name__ == '__main__':
    main()
