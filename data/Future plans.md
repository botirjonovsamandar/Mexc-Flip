# План оптимизации скорости MEXC flip бота

## Context (зачем это)

Бот ловит basis между Binance и MEXC через локальный MetaScalp терминал. Сейчас от "Binance показал спред" до "MEXC ордер исполнен" уходит **600-1500 мс** — это медленнее чем живёт спред на MEXC (200-800 мс), поэтому многие сигналы упускаются.

Где сейчас теряется время (подтверждено аудитом кода):

- **150 мс × 6-10 polls** на ожидание филла через REST `get_orders` ([execution.py:1204](trading_bot/app/execution.py#L1204))
- **50-200 мс** на REST `get_positions` перед каждым входом ([execution.py:233](trading_bot/app/execution.py#L233))
- **50-150 мс** на REST `get_balance` (cache TTL 1с) ([execution.py:722](trading_bot/app/execution.py#L722))
- **~25 мс среднее** на polling `signal_eval_interval_ms=50` ([main.py:267](trading_bot/app/main.py#L267))
- **~50 мс среднее** на polling exit-loop 100мс ([main.py:305](trading_bot/app/main.py#L305))

**Решение пользователя по инфраструктуре (зафиксировано):**
- Остаёмся на текущей локальной Windows-машине — MetaScalp работает только на Windows (8GB RAM + DX11 GPU), Linux не поддерживается. VPS пока не берём
- Прокси не нужен (текущий аккаунт MEXC и интернет провайдер совпадают по гео)
- TG-бот мониторинга не нужен (сделки видны в MetaScalp UI)

**Цель:** уронить полный путь "увидел спред → отправил ордер → знаю про филл" до минимально возможного на текущем хосте (без миграции). Сетевую задержку до бирж (~RTT твоего региона до Tokyo, где matching engines) не убираем — это барьер геолокации, не код. Всё что в коде и архитектуре можно вычистить — вычищаем.

Все факты подтверждены: исходные MetaScalp SDK (`docs/MetaScalp-Api.md` v1.0.6) и независимое research (см. `tidy-dazzling-dream-agent-*.md`).

---

## Архитектурное решение (фундамент)

**Переход с polling-модели на event-driven через MetaScalp WebSocket.** MetaScalp уже отдаёт `order_update`, `position_update`, `balance_update` по WS — бот их не использует для критического пути, а зря. Убираем REST с горячего пути (кроме самого `place_order`).

**Использовать только MetaScalp API** (как просил пользователь). MetaScalp = локальная JSON-over-HTTP/WS на 127.0.0.1:17845-17855, без лимитов на местном слое. Биржевые лимиты остаются — учитываем их.

---

## Оптимизации (по убыванию выигрыша)

### 1. Event-driven signal loop (главный выигрыш)

**Проблема:** [main.py:264-267](trading_bot/app/main.py#L264-L267) — `_signal_loop` спит 50 мс, потом проверяет все 9 символов. Между Binance-апдейтом и оценкой сигнала в среднем теряем **25 мс**.

**Фикс:** В [main.py:_on_ms_event](trading_bot/app/main.py#L214) после обновления `binance_cache` ставить `asyncio.Event` на конкретный символ. `_signal_loop` ждёт `await event.wait()` и проверяет ТОЛЬКО изменившийся символ.

- Файл: [main.py](trading_bot/app/main.py)
- Новая структура: `self._dirty_symbols: set[str]` + `self._dirty_event: asyncio.Event`
- В `_on_ms_event` ветке `source == "binance"`: `self._dirty_symbols.add(canonical); self._dirty_event.set()`
- В `_signal_loop`: `await self._dirty_event.wait(); syms = self._dirty_symbols.copy(); self._dirty_symbols.clear(); self._dirty_event.clear()`
- Минимальный rate-limit: не чаще 1мс между прогонами одного символа (защита от шторма тиков)

**Экономия:** −25 мс среднего, −50 мс p99.

### 2. WS-driven fill confirmation (второй главный выигрыш)

**Проблема:** [execution.py:1192-1233](trading_bot/app/execution.py#L1192-L1233) — `_await_fill` опрашивает `get_orders` каждые 150 мс. **150-1500 мс на каждом входе** просто чтобы узнать о филле, плюс 6-10 REST вызовов забивают rate limiter.

**Фикс:** MetaScalp шлёт `order_update` по WS с полями `OrderId, ClientId, Status, FilledSize, FilledPrice` ([MetaScalp-Api.md §"Order update"](https://raw.githubusercontent.com/MetaScalp/metascalp-sdk/main/docs/MetaScalp-Api.md)). Status принимает `New|Open|Closed`; филлы отличать по `FilledSize == Size` vs `Status == Closed`.

- Файл: [execution.py](trading_bot/app/execution.py), [main.py](trading_bot/app/main.py)
- В `ExecutionEngine` завести `self._pending_order_futures: dict[str, asyncio.Future]` где ключ = `ClientId` (MetaScalp возвращает в ответе на `place_order` — см. [metascalp_client.py:383-390](trading_bot/app/metascalp_client.py#L383-L390))
- `_live_fill` после `place_order` создаёт Future и делает `await asyncio.wait_for(fut, timeout=fill_timeout_ms/1000)`
- В `_on_ms_event` новая ветка `order_update` ищет соответствующий future по `ClientId`/`OrderId` и резолвит с `(FilledPrice, FilledSize, Status)`
- Polling-фолбэк оставить на 500мс (если WS пропустит — редко, но страховка)
- Подписка на account events уже есть ([main.py:197](trading_bot/app/main.py#L197) — `ws.add_connection_subscribe(mexc_conn_id)`), просто события сейчас игнорируются

**Экономия:** −100 мс p50, −500 мс p99 на каждом входе. Заодно убирает 6-10 REST вызовов с rate limiter'а.

### 3. Убрать `refresh_exchange_positions(force=True)` с горячего пути

**Проблема:** [execution.py:233](trading_bot/app/execution.py#L233) — перед каждым `build_entry_plans` принудительный REST `get_positions`.

**Фикс:** WS `position_update` уже триггерит refresh ([main.py:253](trading_bot/app/main.py#L253)). Доверять кешу `self._exchange_positions`, force=True убрать. Фоновый refresh оставить в `_health_loop`.

- Файл: [execution.py:233](trading_bot/app/execution.py#L233) — удалить `await self.refresh_exchange_positions(force=True)`

**Экономия:** −80 мс на каждый сигнал-кандидат.

### 4. Кеш баланса дольше + WS invalidation

**Проблема:** [config.json:88](trading_bot/configs/config.json#L88) `balance_refresh_sec: 1.0` — на каждом сигнале мимо кеша → REST `get_balance` (50-150 мс).

**Фикс:** Баланс меняется только когда мы сами кладём/закрываем сделку или когда WS `balance_update` придёт. 
- Поднять `balance_refresh_sec` до **30**
- В `_on_ms_event` новая ветка `balance_update` инвалидирует `self._account_snapshot = None`
- В `try_enter` после удачного `try_enter`/`close_position` оставить `self._account_snapshot = None` (уже есть в [execution.py:425](trading_bot/app/execution.py#L425), [execution.py:472](trading_bot/app/execution.py#L472))

**Экономия:** −80 мс среднего на сигнал (балансовый REST почти всегда из кеша).

### 5. Event-driven position exit

**Проблема:** [main.py:303-305](trading_bot/app/main.py#L303-L305) — `_position_loop` спит 100 мс. В среднем закрытие позиции запаздывает на 50 мс после изменения mid.

**Фикс:** Триггерить проверку выхода из того же `_dirty_event` (см. #1), но проверять не сигналы а позиции — для каждого символа в `self.positions.open_symbols`, который попал в `dirty_symbols`. Цикл с фиксированным sleep оставить как фолбэк на 1 секунду (timeout-проверки `max_position_time_seconds`).

- Файл: [main.py:303](trading_bot/app/main.py#L303)

**Экономия:** −50 мс на reaction time на выходе.

### 6. msgspec / orjson для парсинга WS

**Проблема:** [metascalp_client.py:503](trading_bot/app/metascalp_client.py#L503) использует стандартный `json.loads`. На 200-500 msg/s это **5-15 мс CPU в секунду** + GC давление. Дополнительно в [main.py:418-520](trading_bot/app/main.py#L418-L520) парсинг orderbook рассыпан по dict.get-chains с двойными PascalCase/camelCase лукапами.

**Фикс:**
- Установить `msgspec` (`pip install msgspec`)
- Определить typed Structs для всех WS payloads: `OrderbookSnapshot`, `OrderbookUpdate`, `OrderUpdate`, `PositionUpdate`, `BalanceUpdate` ([MetaScalp-Api.md §"WebSocket events"](https://raw.githubusercontent.com/MetaScalp/metascalp-sdk/main/docs/MetaScalp-Api.md))
- Подтверждено что MetaScalp шлёт PascalCase ([metascalp_client.py:10](trading_bot/app/metascalp_client.py#L10)) — camelCase fallback'и в `_parse_levels` / `_parse_orderbook_payload` / `_parse_typed_updates` УДАЛИТЬ, определять формат один раз
- Заменить `json.loads(raw)` в [metascalp_client.py:503](trading_bot/app/metascalp_client.py#L503) на `decoder.decode(raw)` где `decoder = msgspec.json.Decoder(MetaScalpMessage)`

**Экономия:** −5 мкс/сообщение на decode, **−15-30 мс CPU в секунду** → освобождает event loop, p99 latency падает на ~5-10 мс.

### 7. BookCache: убрать dict-rebuilds

**Проблема:** [book_cache.py:85-108](trading_bot/app/book_cache.py#L85-L108) — на каждом orderbook update пересобирает оба `dict`'а bid_map/ask_map с нуля и заново сортирует. `~0.5-1 мс` × 200-500 апдейтов в секунду = **100-500 мс CPU/сек на ровном месте**.

**Фикс:** Использовать `sortedcontainers.SortedDict` (`pip install sortedcontainers`) — O(log n) на insert/delete вместо O(n log n) full sort.

- Файл: [book_cache.py:50](trading_bot/app/book_cache.py#L50) (класс `BookCache`) + [book_cache.py:15](trading_bot/app/book_cache.py#L15) (`Book`)
- Альтернатива: ручной `bisect.insort` + `dict` (~такая же производительность, но без зависимости)

**Экономия:** Не на критическом пути одного тика, но снимает CPU нагрузку на 100-400 мс/сек суммарно → ниже p99 на всех операциях.

### 8. BookCache: binary search в `impulse_bps`

**Проблема:** [book_cache.py:149-153](trading_bot/app/book_cache.py#L149-L153) — линейный скан deque до `target_ts`. На 4096 элементах в худшем случае = `~0.5-2 мс` × 3 вызова на сигнал = **1.5-6 мс**.

**Фикс:** Переписать `_mid_history` как `(timestamps: array, mids: array)` через `array.array('d')`, использовать `bisect.bisect_right(timestamps, target_ts)` для O(log n).

- Файл: [book_cache.py:115-156](trading_bot/app/book_cache.py#L115-L156)

**Экономия:** −1-5 мс на сигнал.

### 9. Async CSV/JSON logging

**Проблема:** [logger.py:111-113](trading_bot/app/logger.py#L111-L113) — `CsvSink.write` синхронно открывает файл, держит `threading.Lock`, пишет. **Блокирует event loop на 5-20 мс per write**. Сигнальный лог пишется на КАЖДОМ tick'е через `_record_signal` ([main.py:374-401](trading_bot/app/main.py#L374-L401)) — это в горячем пути.

**Фикс:** Завести `asyncio.Queue` + один воркер-таск, который пишет в фоне. `write()` становится `queue.put_nowait`. То же для `LatencyLogger`.

- Файл: [logger.py:99-137](trading_bot/app/logger.py#L99-L137)
- Применить к `TradesLogger`, `SignalsLogger`, `LatencyLogger`

**Экономия:** убирает 5-20 мс блокировок event loop на каждом write. **−1-5 мс среднего, −20 мс p99** на критическом пути.

### 10. Pre-cached Decimal-степы

**Проблема:** [execution.py:1404-1412](trading_bot/app/execution.py#L1404-L1412) — `Decimal(str(step))` + `Decimal(str(value))` + `to_integral_value` на каждом `_round_price` / `_size_order`. 10-30 мкс × несколько раз на ордер.

**Фикс:** В `TickerRules.from_raw` ([execution.py:112-130](trading_bot/app/execution.py#L112-L130)) сразу сконвертировать `Decimal(str(price_increment))`, `Decimal(str(size_increment))`, число знаков после запятой. Кешировать.

- Файл: [execution.py:106-130](trading_bot/app/execution.py#L106-L130) + [execution.py:1404](trading_bot/app/execution.py#L1404)

**Экономия:** −0.5-1 мс на ордер.

### 11. MetaScalp: `DepthLevels=10` + `DepthPercent=0.5`

**Проблема:** [config.json:29](trading_bot/configs/config.json#L29) `orderbook_depth_levels: 50`. Стратегия использует только top-5 для slippage ([execution.py:1290](trading_bot/app/execution.py#L1290), [execution.py:1310](trading_bot/app/execution.py#L1310)) — остальные 45 уровней мёртвый груз.

**Фикс:** Подписаться с `DepthLevels=10` и `DepthPercent=0.5` — MetaScalp ([MetaScalp-Api.md §"orderbook_subscribe"](https://raw.githubusercontent.com/MetaScalp/metascalp-sdk/main/docs/MetaScalp-Api.md)) обещает что `DepthPercent` фильтрует И snapshot И updates на серверной стороне (band-pass вокруг touch). Рубит **~70-80% payload'а** по WS.

- Файл: [main.py:198-209](trading_bot/app/main.py#L198-L209)
- Конфиг: [config.json:29](trading_bot/configs/config.json#L29)

**Экономия:** −0.3-1 мс на парсинге каждого update + меньше GC давление.

### 12. Application-mode локальные стопы

**Большая фича MetaScalp:** Для типов ордеров `StopLoss` / `TakeProfit` если на коннекшене выставлен режим `Application` (в UI MetaScalp), MetaScalp САМ следит за триггерной ценой локально и шлёт market-ордер на биржу только когда триггер сработал. **Никакого resting стопа на бирже** = защита от stop-out'а на спайках.

- Включить в UI MetaScalp на MEXC коннекшене (один раз руками — API не позволяет)
- В коде уже частично готово: [execution.py:967-969](trading_bot/app/execution.py#L967-L969) (`attached_stop_if_supported`) и [execution.py:1145-1190](trading_bot/app/execution.py#L1145-L1190) (`_place_protective_stop`) — направят триггеры через `OrderType.STOP_LOSS` после фикса конфига
- Конфиг: [config.json:118](trading_bot/configs/config.json#L118) `attached_stop_if_supported: false` → `true`

**Экономия:** На горячий путь напрямую не влияет, но даёт sub-ms реакцию на trigger вместо exchange-side RTT ~150-300 мс. Защита от gap'ов.

### 13. Pre-armed order templates

**Проблема:** В [execution.py:_live_fill](trading_bot/app/execution.py#L938) на каждом входе собирается `OrderRequest`, валидируется ticker, считается limit price, конвертируется в payload — всё блокирующая работа между "решили войти" и `place_order`.

**Фикс:** При старте бота для каждого символа собрать "шаблон" ордера: `{Ticker, Side(placeholder), Type, ReduceOnly}` + предвычисленный native_ticker. На входе только подставлять `Side, Size, Price` и сериализовать через `msgspec.json.Encoder`.

- Файл: [execution.py:938-1115](trading_bot/app/execution.py#L938-L1115)

**Экономия:** −1-3 мс на каждом входе.

### 14. httpx keep-alive limits явно

**Проверить:** [metascalp_client.py:181-184](trading_bot/app/metascalp_client.py#L181-L184) — httpx `AsyncClient` без явных `Limits`. На loopback дефолты обычно ок, но защититься:

```python
limits = httpx.Limits(max_keepalive_connections=20, max_connections=20, keepalive_expiry=300.0)
httpx.AsyncClient(base_url=..., timeout=..., limits=limits)
```

- Файл: [metascalp_client.py:181](trading_bot/app/metascalp_client.py#L181)

**Экономия:** Защита от случайного TCP reconnect'а — ~50-200 мкс на запрос когда срабатывает.

### 15. Реалистичные rate-limit лимиты

**Проблема:** [rate_limiter.py](trading_bot/app/rate_limiter.py) глобально ограничивает все запросы к MetaScalp по `max_mexc_requests_per_sec: 6` и `_per_min: 180`. Но **MetaScalp сам по себе rate limit не накладывает** ([MetaScalp-Api.md](https://raw.githubusercontent.com/MetaScalp/metascalp-sdk/main/docs/MetaScalp-Api.md)). Биржевые лимиты применяются только к тем запросам что реально идут на биржу. После фикса #2 polling-вызовы `get_orders` исчезают и текущие лимиты становятся слишком жёсткими.

**Фикс:** 
- [config.json:74-75](trading_bot/configs/config.json#L74-L75): `max_mexc_requests_per_sec: 6` → `15`, `max_mexc_requests_per_min: 180` → `400`
- [config.json:103](trading_bot/configs/config.json#L103): `upstream_hourly_limit: 500` → `5000` (после подтверждения реального лимита MEXC под твою VIP ставку)

**Экономия:** Убирает ложные блокировки в `_await_fill`/`_live_close`. Не латенси сам по себе, но снимает edge-cases.

### 16. Windows тюнинг (бесплатно)

- Process priority бота и MetaScalp = **High** (не Realtime — Realtime ломает планировщик). Запустить через `Start-Process -Priority High` или через ярлык с админ-правами
- CPU affinity: pin бот на 2 core'а, MetaScalp на другие 2 core'а, OS на core 0 (`Set-ProcessAffinity` или Process Lasso)
- **Disable Windows Defender real-time scanning** на папке `c:\Users\User\Desktop\Soft\Mexc flip\` и MetaScalp installation dir — экономия 100-500 мкс/файл
- В Device Manager → NIC → Advanced: `Interrupt Moderation = Disabled`, `Receive Side Scaling = Enabled`, `Receive Buffers = Max`
- Disable NIC power saving ("Allow the computer to turn off this device to save power" → uncheck)
- В Power Plan: `High performance` (не Balanced)

**Экономия:** −1-3 мс p99 tail.

---

## Файлы которые меняем

| Файл | Что |
|------|-----|
| [main.py:264-301](trading_bot/app/main.py#L264-L301) | `_signal_loop` event-driven (#1) |
| [main.py:303-313](trading_bot/app/main.py#L303-L313) | `_position_loop` event-driven (#5) |
| [main.py:214-260](trading_bot/app/main.py#L214-L260) | Обработка `order_update`/`balance_update`/`position_update` → резолв futures + invalidate cache |
| [main.py:198-209](trading_bot/app/main.py#L198-L209) | `DepthLevels=10` + `DepthPercent=0.5` (#11) |
| [main.py:418-520](trading_bot/app/main.py#L418-L520) | Убрать camelCase fallback'и (#6), оставить только PascalCase |
| [execution.py:233](trading_bot/app/execution.py#L233) | Удалить `refresh_exchange_positions(force=True)` (#3) |
| [execution.py:1192-1233](trading_bot/app/execution.py#L1192-L1233) | `_await_fill` → ждёт Future от WS, polling-фолбэк 500мс (#2) |
| [execution.py:938-1115](trading_bot/app/execution.py#L938-L1115) | `_live_fill` создаёт Future перед `place_order`, pre-armed templates (#13) |
| [execution.py:106-130, 1404](trading_bot/app/execution.py#L106-L130) | `TickerRules` кеширует pre-computed Decimal (#10) |
| [book_cache.py:50-160](trading_bot/app/book_cache.py#L50-L160) | `BookCache` через `SortedDict` (#7), `impulse_bps` через `bisect` (#8) |
| [logger.py:99-137](trading_bot/app/logger.py#L99-L137) | Async-очередь для CSV/JSON write (#9) |
| [metascalp_client.py:181](trading_bot/app/metascalp_client.py#L181) | httpx Limits явно (#14) |
| [metascalp_client.py:503](trading_bot/app/metascalp_client.py#L503) | `msgspec.json.Decoder` вместо `json.loads` (#6) |
| [config.json:29](trading_bot/configs/config.json#L29) | `orderbook_depth_levels: 50` → `10` |
| [config.json:74-75](trading_bot/configs/config.json#L74-L75) | MEXC limits → реалистичные (#15) |
| [config.json:88](trading_bot/configs/config.json#L88) | `balance_refresh_sec: 1.0` → `30.0` |
| [config.json:103](trading_bot/configs/config.json#L103) | `upstream_hourly_limit: 500` → `5000` |
| [config.json:118](trading_bot/configs/config.json#L118) | `attached_stop_if_supported: false` → `true` (после включения Application mode в UI MetaScalp) |
| `requirements.txt` | `+msgspec`, `+sortedcontainers` |

## Существующее что переиспользуем

- **WS event handler** ([main.py:214](trading_bot/app/main.py#L214)) — расширяем а не переписываем
- **`MetaScalpWS`** ([metascalp_client.py:426](trading_bot/app/metascalp_client.py#L426)) — уже подписан на account events для MEXC ([main.py:197](trading_bot/app/main.py#L197)), просто начинаем обрабатывать `order_update`
- **`OrderRequest`** ([metascalp_client.py:128](trading_bot/app/metascalp_client.py#L128)) — основа для pre-armed templates
- **`TickerRules`** ([execution.py:106](trading_bot/app/execution.py#L106)) — расширяем pre-computed Decimal
- **`RateLimiter`** ([rate_limiter.py](trading_bot/app/rate_limiter.py)) — оставляем, корректные лимиты в конфиге
- **`CooldownTracker`** ([strategy.py:67](trading_bot/app/strategy.py#L67)) — без изменений
- **`_pending_entries`** dict ([execution.py:159](trading_bot/app/execution.py#L159)) — паттерн уже есть для marginных резерваций, по аналогии делаем `_pending_order_futures`

---

## Verification (как проверить end-to-end)

1. **Unit тесты:**
   - `tests/test_book_cache.py` — новая `SortedDict`-реализация даёт те же `best_bid`/`best_ask`/`mid`/`spread_bps` на тех же входах. `impulse_bps` идентичен старой реализации
   - Запустить `pytest` ([trading_bot/pytest.ini](trading_bot/pytest.ini))

2. **DRY_RUN режим** ([config.json:2](trading_bot/configs/config.json#L2) `mode: "DRY_RUN"`):
   - `python -m app.main --config configs/config.json`
   - Дашборд `http://127.0.0.1:8765` показывает сигналы как и раньше
   - В `signals.csv` сравнить decision-rate до/после — должно вырасти (event-driven ловит больше тиков)
   - В логах появляются `metascalp.event type=order_update` при наличии активных позиций на бирже

3. **PAPER режим:**
   - Симулирует филл по best bid/ask. Latency `latency_order_send`/`latency_fill` в `trades.csv` должны быть **0**
   - Проверить что бот не падает при отсутствии WS `order_update` событий (в PAPER их нет — должен корректно использовать симулированный филл)

4. **SMALL_LIVE на минимальном размере (5 USDT):**
   - Сделать 5-10 сделок
   - В `data/logs/latency.log` смотреть распределение `latency_send_ms` и `latency_fill_ms`
   - **Acceptance**: средний `latency_send_ms` < 150мс (текущий 50-150), средний `latency_fill_ms` < **50мс** (текущий 150-1500)
   - Если `latency_fill_ms` > 200мс — WS-роутинг филлов сломан, чекать что подписка на `subscribe` MEXC connection_id активна и `order_update` приходят

5. **Регрессия на отказах:**
   - Прибить MetaScalp на 30 сек — бот корректно реконнектится, polling-фолбэк в `_await_fill` срабатывает если WS не вернётся вовремя
   - Открыть позицию вручную в MetaScalp UI — `_health_loop` подхватывает через `position_update`

6. **Latency benchmark скрипт** (новый, `tests/bench_hot_path.py`):
   - Симулировать 1000 синтетических orderbook updates
   - Засечь время от `_on_ms_event` до момента когда `_signal_loop` дёрнул `evaluate()`
   - Acceptance: p50 < 2 мс, p99 < 5 мс

---

## Итоговая таблица улучшений и экономия мс

| # | Улучшение | Где | Экономия |
|---|-----------|-----|----------|
| 1 | Event-driven signal loop | [main.py:264](trading_bot/app/main.py#L264) | **−25 мс avg, −50 p99** |
| 2 | WS fill confirmation (убрать polling) | [execution.py:1192](trading_bot/app/execution.py#L1192) | **−100 p50, −500 p99 per entry** |
| 3 | Убрать force REST get_positions | [execution.py:233](trading_bot/app/execution.py#L233) | **−80 мс per signal** |
| 4 | Кеш баланса 30с + WS invalidation | [config.json:88](trading_bot/configs/config.json#L88) | **−80 мс avg per signal** |
| 5 | Event-driven position exit | [main.py:303](trading_bot/app/main.py#L303) | **−50 мс reaction time** |
| 6 | msgspec + remove camelCase fallback | [metascalp_client.py:503](trading_bot/app/metascalp_client.py#L503) | **−15-30 мс/сек CPU, −5-10 p99** |
| 7 | BookCache без dict-rebuild | [book_cache.py:85](trading_bot/app/book_cache.py#L85) | **−100-400 мс/сек CPU** |
| 8 | Binary search в impulse_bps | [book_cache.py:142](trading_bot/app/book_cache.py#L142) | **−1-5 мс per signal** |
| 9 | Async CSV logging | [logger.py:111](trading_bot/app/logger.py#L111) | **−1-5 мс avg, −20 p99** |
| 10 | Pre-cached Decimal steps | [execution.py:1404](trading_bot/app/execution.py#L1404) | −0.5-1 мс per order |
| 11 | DepthLevels=10 + DepthPercent=0.5 | [main.py:198](trading_bot/app/main.py#L198) | **−0.3-1 мс/update** |
| 12 | Application-mode локальные стопы | UI MetaScalp + [config.json:118](trading_bot/configs/config.json#L118) | защита от stop-out на спайках |
| 13 | Pre-armed order templates | [execution.py:938](trading_bot/app/execution.py#L938) | **−1-3 мс per entry** |
| 14 | httpx keep-alive limits | [metascalp_client.py:181](trading_bot/app/metascalp_client.py#L181) | −50-200 мкс защита |
| 15 | Реалистичные rate-limit | [config.json:74](trading_bot/configs/config.json#L74) | убирает ложные блоки |
| 16 | Windows priority/affinity/Defender | OS | **−1-3 мс p99** |

### Итоговый бюджет latency на критическом пути "Binance edge → MEXC hedge"

| Состояние | p50 | p99 | Комментарий |
|-----------|-----|-----|------|
| **Сейчас** (polling, REST get_orders fill) | **600-900 мс** | **1500+ мс** | Большинство спредов упускается |
| **После всех 16 фиксов** (на твоей текущей машине) | **300-400 мс** | **500-700 мс** | Уже ловишь спреды живущие >300мс |

**Чистая экономия от плана:** −300 до −500 мс p50, −500 до −1000 мс p99.

**Что барьером остаётся:** сетевой RTT из твоего региона до Tokyo (где Binance и MEXC matching engines). Это **физика геолокации**, не код. Чтобы пробить — нужен Windows VPS в Tokyo + прокси под гео твоего аккаунта для MEXC compliance, но это ~$10-15/мес (MetaScalp требует Windows + 8GB RAM, cheap Linux VPS не подходит) — отложено до момента когда стратегия докажет прибыльность.

**Главные выводы:**
- Все основные оптимизации **бесплатны** и не требуют смены инфраструктуры
- Точка соприкосновения с реальностью: после фиксов спреды что живут 300-500мс начнёшь брать стабильно (сейчас ловишь только 800+мс хвосты)
- Если позже стратегия начнёт показывать profit и захочешь развиваться дальше — миграция на Tokyo Windows VPS ($10-15/мес Contabo) + прокси под гео аккаунта даст ещё −250 мс. Но это уже после того как validate'ом подтвердишь edge на текущем стенде
