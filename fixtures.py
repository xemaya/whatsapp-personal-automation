import time
from store import DEMO_GROUPS


def demo_events():
    now = int(time.time())
    examples = [
        ('demo-s1', 'Ava', 'demo-seller-1', '出售两人沙发，伦敦自提，£80，九成新。', DEMO_GROUPS[0]),
        ('demo-w1', 'Ben', 'demo-buyer-1', '求购两人沙发，伦敦，预算£100。', DEMO_GROUPS[0]),
        ('demo-s2', 'Chloe', 'demo-seller-2', 'Selling a desk in London. £45, collection this weekend.', DEMO_GROUPS[1]),
        ('demo-w2', 'David', 'demo-buyer-2', 'WTB desk in London. Budget £60.', DEMO_GROUPS[1]),
        ('demo-city', 'Eli', 'demo-buyer-3', '求购沙发，曼城，预算£100。', DEMO_GROUPS[0]),
        ('demo-budget', 'Faye', 'demo-buyer-4', '求购沙发，伦敦，预算£50。', DEMO_GROUPS[0]),
        ('demo-phone1', 'Gia', 'demo-seller-3', '出售 iPhone 14 Pro，伦敦，£350。', DEMO_GROUPS[0]),
        ('demo-phone2', 'Hugo', 'demo-buyer-5', '求购 iPhone 15 Pro，伦敦，预算£500。', DEMO_GROUPS[0]),
        ('demo-chat', 'Ivy', 'demo-member-1', '今天群里有人去公园吗？', DEMO_GROUPS[0]),
    ]
    messages = [{'id': mid, 'chat_id': group, 'from': sender, 'from_name': name, 'from_me': False,
                 'timestamp': now, 'type': 'text', 'text': {'body': text}, 'source': 'mobile'}
                for mid, name, sender, text, group in examples]
    messages.extend([
        {'id': 'demo-private', 'chat_id': '123456789@s.whatsapp.net', 'from': 'x', 'from_me': False,
         'timestamp': now, 'text': {'body': '出售沙发 £10'}},
        {'id': 'demo-self', 'chat_id': DEMO_GROUPS[0], 'from': 'self', 'from_me': True,
         'timestamp': now, 'text': {'body': '出售沙发 £20'}},
    ])
    # Stable bodies across replay calls let tests prove message-level deduplication too.
    return [{'channel_id': 'DEMO-LOCAL', 'event': {'type': 'messages', 'event': 'post'}, 'messages': messages}]
