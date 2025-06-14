using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using MAG7.TradingSystem.Models;
using MAG7.TradingSystem.Data;

namespace MAG7.TradingSystem.Brokers.Tasty
{
    public class TastyTradeQuoteClient
    {
        private readonly Uri _webSocketUri;
        private readonly string[] _symbols;
        private readonly TimescaleDbWriter _dbWriter;

        public TastyTradeQuoteClient(string webSocketUrl, string[] symbols, TimescaleDbWriter dbWriter)
        {
            _webSocketUri = new Uri(webSocketUrl);
            _symbols = symbols;
            _dbWriter = dbWriter;
        }

        #pragma warning disable CS1998
        public async Task StartAsync(string sessionToken)
        {
            await StartAsync(sessionToken, async (_) => { });
        }

        public async Task StartAsync(string sessionToken, Func<string, Task> onTickReceived)
        {
            while (true)
            {
                try
                {
                    using var socket = new ClientWebSocket();
                    await socket.ConnectAsync(_webSocketUri, CancellationToken.None);

                    await SendJsonAsync(socket, new
                    {
                        action = "authenticate",
                        args = new { token = sessionToken }
                    });

                    await SendJsonAsync(socket, new
                    {
                        action = "subscribe",
                        args = new { channels = _symbols.Select(sym => $"quotes:{sym}").ToArray() }
                    });

                    var buffer = new byte[8192];

                    while (socket.State == WebSocketState.Open)
                    {
                        var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                        var json = Encoding.UTF8.GetString(buffer, 0, result.Count);

                        // ✅ Forward to WebSocket clients
                        await onTickReceived(json);

                        try
                        {
                            using var doc = JsonDocument.Parse(json);
                            if (doc.RootElement.TryGetProperty("data", out var data))
                            {
                                string symbol = data.GetProperty("symbol").GetString() ?? "UNKNOWN";

                                var tick = new QuoteTick
                                {
                                    Symbol = symbol,
                                    Bid = data.GetProperty("bid-price").GetDouble(),
                                    Ask = data.GetProperty("ask-price").GetDouble(),
                                    Last = data.GetProperty("last-price").GetDouble(),
                                    Volume = data.GetProperty("volume").GetInt64(),
                                    Timestamp = DateTime.UtcNow
                                };

                                await _dbWriter.InsertQuoteTickAsync(tick);
                                await _dbWriter.InsertRawQuoteJsonAsync(symbol, json);

                                Console.WriteLine($"{tick.Symbol} @ {tick.Last}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[PARSE ERROR] {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[STREAM ERROR] {ex.Message}");
                    await Task.Delay(3000);
                }
            }
        }

        private async Task SendJsonAsync(ClientWebSocket socket, object payload)
        {
            var json = JsonSerializer.Serialize(payload);
            var bytes = Encoding.UTF8.GetBytes(json);
            var buffer = new ArraySegment<byte>(bytes);
            await socket.SendAsync(buffer, WebSocketMessageType.Text, true, CancellationToken.None);
        }
    }
}
