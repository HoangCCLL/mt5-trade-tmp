"""In-memory stand-in for the MetaTrader5 package: enough surface for trader/streamer, deterministic, Linux-friendly."""
import random, time
from collections import Counter, namedtuple
from datetime import datetime

TRADE_ACTION_DEAL, TRADE_ACTION_SLTP = 1, 6
ORDER_TYPE_BUY, ORDER_TYPE_SELL = 0, 1
ORDER_TIME_GTC = 0
ORDER_FILLING_FOK, ORDER_FILLING_IOC, ORDER_FILLING_RETURN = 0, 1, 2
SYMBOL_FILLING_FOK, SYMBOL_FILLING_IOC = 1, 2
TRADE_RETCODE_DONE, TRADE_RETCODE_MARKET_CLOSED, TRADE_RETCODE_INVALID_FILL = 10009, 10018, 10030
DEAL_ENTRY_IN, DEAL_ENTRY_OUT = 0, 1

TerminalInfo = namedtuple("TerminalInfo", "connected trade_allowed")
AccountInfo = namedtuple("AccountInfo", "login balance equity margin margin_free margin_level profit currency leverage company server")
SymbolInfo = namedtuple("SymbolInfo", "name digits point filling_mode trade_stops_level volume_min volume_step volume_max trade_contract_size currency_base currency_profit")
Tick = namedtuple("Tick", "time bid ask last volume time_msc")
Position = namedtuple("Position", "ticket time type magic symbol volume price_open sl tp price_current profit swap comment")
Deal = namedtuple("Deal", "ticket order time time_msc type entry magic position_id symbol volume price commission swap profit comment")
CheckResult = namedtuple("CheckResult", "retcode comment margin margin_free")
SendResult = namedtuple("SendResult", "retcode deal order volume price comment request_id")

calls: Counter = Counter()
order_send_delay = 0.0
clock_offset = 0.0  # seconds the simulated broker clock is ahead of the local clock
market_closed = False
freeze_ticks = False
_positions: dict[int, Position] = {}
_deals: list[Deal] = []
_prices: dict[str, float] = {}
_last_tick: dict[str, Tick] = {}
_seq = 0


def reset():
    global order_send_delay, market_closed, freeze_ticks, clock_offset, _seq
    calls.clear(); _positions.clear(); _deals.clear(); _prices.clear(); _last_tick.clear()
    order_send_delay, market_closed, freeze_ticks, clock_offset, _seq = 0.0, False, False, 0.0, 0


def _next():
    global _seq
    _seq += 1
    return _seq


def _mid(symbol):
    return _prices.setdefault(symbol, 1.10000 if symbol.startswith("EUR") else 2000.0)


def initialize(**_):
    calls["initialize"] += 1
    return True


def shutdown():
    pass


def last_error():
    return (0, "")


def terminal_info():
    return TerminalInfo(connected=True, trade_allowed=True)


def account_info():
    profit = sum(p.profit for p in _positions.values())
    return AccountInfo(login=1, balance=10000.0, equity=10000.0 + profit, margin=0.0, margin_free=10000.0, margin_level=0.0,
                       profit=profit, currency="USD", leverage=100, company="Fake Broker Ltd", server="Fake-Demo")


def order_calc_margin(action, symbol, volume, price):
    return volume * symbol_info(symbol).trade_contract_size * price / account_info().leverage


def symbols_get(group=None):
    calls["symbols_get"] += 1
    return tuple(symbol_info(n) for n in ("EURUSD", "GBPUSD", "XAUUSD"))


def symbol_info(symbol):
    return SymbolInfo(name=symbol, digits=5, point=0.00001, filling_mode=SYMBOL_FILLING_FOK, trade_stops_level=0,
                      volume_min=0.01, volume_step=0.01, volume_max=100, trade_contract_size=100 if symbol.startswith("XAU") else 100000,
                      currency_base=symbol[:3], currency_profit=symbol[3:6])


def symbol_select(symbol, enable=True):
    return True


def symbol_info_tick(symbol):
    calls["symbol_info_tick"] += 1
    if freeze_ticks and symbol in _last_tick:
        return _last_tick[symbol]
    _prices[symbol] = _mid(symbol) + random.uniform(-0.00005, 0.00005)
    now = time.time() + clock_offset  # broker server clock, not UTC
    t = Tick(time=int(now), bid=round(_prices[symbol] - 0.00005, 5), ask=round(_prices[symbol] + 0.00005, 5),
             last=0.0, volume=0, time_msc=int(now * 1000))
    _last_tick[symbol] = t
    return t


def positions_get(ticket=None, symbol=None):
    calls["positions_get"] += 1
    out = []
    for p in _positions.values():
        if ticket is not None and p.ticket != ticket:
            continue
        if symbol is not None and p.symbol != symbol:
            continue
        cur = _mid(p.symbol)
        sign = 1 if p.type == ORDER_TYPE_BUY else -1
        out.append(p._replace(price_current=cur, profit=round(sign * (cur - p.price_open) * p.volume * 100000, 2)))
    return tuple(out)


def history_deals_get(date_from, date_to):
    calls["history_deals_get"] += 1
    f = date_from.timestamp() if isinstance(date_from, datetime) else float(date_from)
    t = date_to.timestamp() if isinstance(date_to, datetime) else float(date_to)
    return tuple(d for d in _deals if f <= d.time <= t)


def order_check(request):
    calls["order_check"] += 1
    if market_closed:
        return CheckResult(TRADE_RETCODE_MARKET_CLOSED, "Market closed", 0.0, 10000.0)
    return CheckResult(0, "Done", 0.0, 10000.0)


def order_send(request):
    calls["order_send"] += 1
    if order_send_delay:
        time.sleep(order_send_delay)
    if market_closed:
        return SendResult(TRADE_RETCODE_MARKET_CLOSED, 0, 0, 0.0, 0.0, "Market closed", 0)
    now = time.time() + clock_offset  # broker server clock, not UTC
    if request["action"] == TRADE_ACTION_SLTP:
        p = _positions[request["position"]]
        _positions[p.ticket] = p._replace(sl=request.get("sl", 0.0), tp=request.get("tp", 0.0))
        return SendResult(TRADE_RETCODE_DONE, 0, p.ticket, p.volume, p.price_open, "Done", 0)
    symbol, vol, typ = request["symbol"], float(request["volume"]), request["type"]
    tick = symbol_info_tick(symbol)
    price = tick.ask if typ == ORDER_TYPE_BUY else tick.bid
    order = _next()
    if "position" in request:  # close (full or partial)
        p = _positions[request["position"]]
        vol = min(vol, p.volume)
        sign = 1 if p.type == ORDER_TYPE_BUY else -1
        profit = round(sign * (price - p.price_open) * vol * 100000, 2)
        deal = Deal(_next(), order, int(now), int(now * 1000), typ, DEAL_ENTRY_OUT, p.magic, p.ticket, symbol,
                    vol, price, 0.0, 0.0, profit, request.get("comment", ""))
        _deals.append(deal)
        if vol >= p.volume:
            del _positions[p.ticket]
        else:
            _positions[p.ticket] = p._replace(volume=round(p.volume - vol, 2))
        return SendResult(TRADE_RETCODE_DONE, deal.ticket, order, vol, price, "Done", 0)
    ticket = _next()
    _positions[ticket] = Position(ticket, int(now), typ, request.get("magic", 0), symbol, vol, price,
                                  request.get("sl", 0.0), request.get("tp", 0.0), price, 0.0, 0.0,
                                  request.get("comment", ""))
    deal = Deal(_next(), order, int(now), int(now * 1000), typ, DEAL_ENTRY_IN, request.get("magic", 0), ticket,
                symbol, vol, price, 0.0, 0.0, 0.0, request.get("comment", ""))
    _deals.append(deal)
    return SendResult(TRADE_RETCODE_DONE, deal.ticket, ticket, vol, price, "Done", 0)
