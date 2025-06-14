# MAG7 System Masterplan

## 📌 Core Objectives
- Real-time data streaming for stocks, futures, and options
- Strategy development and testing: MAG7, Fat Tail, Convexity, London/NY sessions
- Modular broker integration (TastyTrade, Alpaca)
- TimescaleDB for tick storage + analytics
- C# WebSocket proxy to React frontend
- Tabbed professional UI (quotes, chains, playbooks)
- Daily edge detection: 3-day high/low, delta clustering, GEX zones
- Optional LLM analysis for trade logs + convex setups

---

## ✅ Phase 1: Core Infrastructure
- [x] TastyTrade login + token cache
- [x] TimescaleDB insert (quotes, raw json)
- [x] WebSocket broadcast via QuoteStreamHub
- [x] Multi-symbol streaming based on config
- [x] Logging of ticks to disk by date

## ✅ Phase 2: UI + Frontend Display
- [x] Tabbed UI with tick display
- [ ] Options panel with Greeks
- [ ] Drop-down to select symbols
- [ ] Highlight MAG7 targets (vol > X, IV rank > Y)

## ✅ Phase 3: Strategy Layers
- [ ] MAG7: entry, risk, 2-contract split, mid + trail
- [ ] Fat Tail: < 0.50 convex OTM flys on 2-3σ edges
- [ ] Convexity sweep: detect unusual skew, IV shape
- [ ] NY session playbook + YouTube candle signals
- [ ] London session ORB, ICT liquidity sweep

## ✅ Phase 4: Automation + Monitoring
- [ ] Log trades manually + auto-sim
- [ ] Alerts on convex setups or tail compression
- [ ] Strategy metrics: win rate, avg hold time, edge value
- [ ] Account state tracking (balance, drawdown, %R)

## ✅ Phase 5: Research + Expansion
- [ ] Alpaca historical data for strategy backfill
- [ ] Symbol analyzer: earnings, macro, correlation clusters
- [ ] Sector ETF + index modeling engine
- [ ] Heatmap for GEX vs IV vs relVol

---

## 🤖 Future Wishlist
- [ ] Replay mode with UI scrubber
- [ ] AI suggestion engine for best edge by time
- [ ] Swing setups (overnight gamma, sector rotation)
- [ ] Mobile UI for alerts + dashboard

---

All components live in GitHub: `CSharp_Mag7_Machine`

> "One tick at a time. Build the edge. Execute without hesitation."
