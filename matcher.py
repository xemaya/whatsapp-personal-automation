"""Explainable local rules, deliberately conservative. No model/network calls."""
import re
import unicodedata

ITEMS = {
    'sofa': ('沙发', r'沙发|\bsofa\b|\bcouch\b'),
    'desk': ('书桌', r'书桌|办公桌|电脑桌|\bdesk\b'),
    'chair': ('椅子', r'椅子|办公椅|\bchair\b'),
    'bike': ('自行车', r'自行车|单车|\bbicycle\b|\bbike\b'),
    'fridge': ('冰箱', r'冰箱|\bfridge\b|\brefrigerator\b'),
    'monitor': ('显示器', r'显示器|\bmonitor\b'),
    'iphone': ('iPhone', r'\biphone\s*\d*|苹果手机'),
    'stroller': ('婴儿车', r'婴儿车|\bstroller\b|\bpram\b'),
    'suitcase': ('行李箱', r'行李箱|\bsuitcase\b'),
}
CITIES = {'伦敦': r'伦敦|\blondon\b', '曼城': r'曼城|曼彻斯特|\bmanchester\b',
          '纽约': r'纽约|\bnew york\b|\bnyc\b', '新加坡': r'新加坡|\bsingapore\b',
          '上海': r'上海|\bshanghai\b', '北京': r'北京|\bbeijing\b'}
CURRENCIES = {'£': 'GBP', 'gbp': 'GBP', '$': 'USD', 'usd': 'USD',
              's$': 'SGD', 'sgd': 'SGD', '¥': 'CNY', '￥': 'CNY',
              'rmb': 'CNY', 'cny': 'CNY', '元': 'CNY', '镑': 'GBP', '英镑': 'GBP'}


def extract(text):
    t = unicodedata.normalize('NFKC', text).lower()
    if re.search(r'已售|已出|sold\b|不卖了|撤销求购|已买到|不收了|取消求购', t):
        return {'intent': 'inactive', 'reason': '正文标记已结束'}
    offer = bool(re.search(r'出售|转让|闲置|出一|出个|出台|^出\s|^卖|\bfor sale\b|\bselling\b|\bwts\b', t))
    wanted = bool(re.search(r'求购|想买|想收|收一|收个|收台|^收\s|\bwtb\b|\blooking for\b|\bwant to buy\b', t))
    if re.search(r'不是出售|不出售|不是卖|not for sale', t):
        offer = False
    intent = 'offer' if offer and not wanted else 'wanted' if wanted and not offer else 'review'
    items = [(k, label) for k, (label, pattern) in ITEMS.items() if re.search(pattern, t)]
    item, label = items[0] if len(items) == 1 else (None, None)
    cities = [city for city, pat in CITIES.items() if re.search(pat, t)]
    city = cities[0] if len(cities) == 1 else None
    price = currency = None
    # Only currency-marked amounts; bare digits must not turn an iPhone model into a price.
    patterns = [r'(s\$|sgd|gbp|usd|rmb|cny|£|\$|¥|￥)\s*([\d,]+(?:\.\d{1,2})?)',
                r'([\d,]+(?:\.\d{1,2})?)\s*(英镑|镑|元|gbp|usd|sgd|rmb|cny)']
    found = []
    for i, pat in enumerate(patterns):
        for m in re.finditer(pat, t):
            sym, num = (m.group(1), m.group(2)) if i == 0 else (m.group(2), m.group(1))
            found.append((float(num.replace(',', '')), CURRENCIES.get(sym)))
    if len(set(found)) == 1:
        price, currency = found[0]
    model = None
    if item == 'iphone':
        m = re.search(r'iphone\s*(\d{1,2})(?:\s*(pro\s*max|pro|plus|mini))?', t)
        if m:
            model = m.group(1) + ((' ' + m.group(2)) if m.group(2) else '')
    reason = '可识别供需' if intent in ('offer', 'wanted') and item else '信息不足或含多种商品，待人工查看'
    return {'intent': intent, 'item': item, 'item_label': label, 'city': city,
            'price': price, 'currency': currency, 'model': model, 'reason': reason}


def match_pair(a, b):
    """Return a candidate, never a send instruction. Unknown facts are explicit."""
    x, y = a['extracted'], b['extracted']
    if {x.get('intent'), y.get('intent')} != {'offer', 'wanted'}:
        return None
    seller, buyer = (a, b) if x['intent'] == 'offer' else (b, a)
    s, w = seller['extracted'], buyer['extracted']
    if not s.get('item') or s['item'] != w.get('item'):
        return None
    if seller.get('sender_id') == buyer.get('sender_id'):
        return None
    if s.get('city') and w.get('city') and s['city'] != w['city']:
        return None
    if s.get('model') and w.get('model') and s['model'] != w['model']:
        return None
    if s.get('price') is not None and w.get('price') is not None:
        if s.get('currency') != w.get('currency') or s['price'] > w['price']:
            return None
    reasons = ['商品一致：' + s['item_label']]
    unknown = []
    if s.get('city') and w.get('city'):
        reasons.append('城市一致：' + s['city'])
    else:
        unknown.append('城市需确认')
    if s.get('price') is not None and w.get('price') is not None:
        reasons.append(f"报价 {s['price']:g} ≤ 预算 {w['price']:g} {s['currency']}")
    else:
        unknown.append('价格或预算未说明')
    if s.get('item') == 'iphone' and (not s.get('model') or not w.get('model')):
        unknown.append('手机具体型号需确认')
    return {'offer_key': seller['key'], 'wanted_key': buyer['key'], 'item': s['item_label'],
            'reasons': reasons, 'unknown': unknown, 'confidence': '需确认' if unknown else '规则吻合',
            'draft': f"发现一条可能匹配的{s['item_label']}信息。请双方确认成色、规格、地点和价格后再联系。"}
