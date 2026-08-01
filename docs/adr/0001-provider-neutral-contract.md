# ADR 0001: Provider-neutral stream contract

## Status

Accepted.

## Context

Axion Trading Core currently consumes Bybit linear-perpetual ticker and one-minute candle JSON
directly. Other venues divide the same observations across different channels and use incompatible
symbol, timestamp, sequence, volume, and candle-finality rules. A Bybit-shaped gateway response
would hide those differences without removing them.

## Decision

The gateway exposes a versioned JSON WebSocket contract owned by Axion rather than any exchange.
Every event is qualified by provider, product, exact venue symbol, connection epoch, receive time,
and optional source sequence. Financial values remain decimal strings. Ticker fields carry their
own observation times because last, mark, index, funding, and best prices can come from independent
upstream messages.

Version 1 supports USDT/linear perpetual ticker snapshots and one-minute candles. Subscriptions use
an explicit provider and venue symbol. The gateway does not select a venue or silently fail over an
open trade; that is a replayable Trading Core policy decision.

Candle `end_time_ms` is exclusive. Candle volume is divided into base, quote, and contract units.
Unknown or unreliable values are omitted instead of guessed. In particular, the gateway never
synthesizes a derivative mark price from the last trade price.

## Consequences

The initial Core client will translate normalized events directly into Core domain objects instead
of reconstructing fake Bybit messages. Core must become venue-aware before multiple providers can
write authoritatively. The contract can later gain spot, trades, books, catalog, and history APIs
without changing the meaning of v1 fields.

WebSocket JSON is the first downstream transport because Core already operates an asynchronous
WebSocket client and the upstream feeds are JSON. The domain contract is transport-independent; a
Protobuf/gRPC projection can be added if profiling demonstrates a material benefit.

