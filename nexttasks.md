# MAG7 System - Next Tasks

## 📅 Current Session Target
- [ ] Build `TastyTradeOptionClient.cs` to fetch options chain via `/option-chains/{symbol}/nested`
- [ ] Test response using `SPY` or `AAPL`
- [ ] Parse into option contracts with:
  - Expiration
  - Strike
  - Type (call/put)
  - Bid/Ask, Mid
  - Greeks (Delta, Gamma, Theta, Vega)
  - Probability ITM
- [ ] Design raw display logic for first UI tab

## 🔜 Queued Items
- [ ] Build UI tab to display selected option chain
- [ ] Add dropdown symbol selector (MAG7 list)
- [ ] Enable user to toggle call/put, expiry filters
- [ ] Add table sorting + pagination if needed

## ✅ Recently Completed
- [x] Backend stream via TastyTrade WebSocket
- [x] Tick broadcast over local WebSocket hub
- [x] TimescaleDB tick + raw logging
- [x] Frontend WebSocket connection confirmed
- [x] GitHub push to public repo

> Ask me "What are we working on next?" and I’ll pull from here with updates.
