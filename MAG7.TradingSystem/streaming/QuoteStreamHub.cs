using Microsoft.AspNetCore.Http;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace MAG7.TradingSystem.Streaming
{
    public class QuoteStreamHub
    {
        private readonly ConcurrentDictionary<string, WebSocket> _clients = new();

        public async Task HandleClientAsync(HttpContext context, WebSocket webSocket)
        {
            var clientId = Guid.NewGuid().ToString();
            _clients[clientId] = webSocket;
            Console.WriteLine($"[WS] Client connected: {clientId}");

            var buffer = new byte[1024 * 4];

            try
            {
                while (webSocket.State == WebSocketState.Open)
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Close)
                        break;
                }
            }
            finally
            {
                _clients.TryRemove(clientId, out _);
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Bye", CancellationToken.None);
                Console.WriteLine($"[WS] Client disconnected: {clientId}");
            }
        }

        public async Task BroadcastAsync(object message)
        {
            var json = JsonSerializer.Serialize(message);
            var buffer = Encoding.UTF8.GetBytes(json);
            var segment = new ArraySegment<byte>(buffer);

            foreach (var client in _clients)
            {
                if (client.Value.State == WebSocketState.Open)
                {
                    try
                    {
                        await client.Value.SendAsync(segment, WebSocketMessageType.Text, true, CancellationToken.None);
                    }
                    catch
                    {
                        Console.WriteLine($"[WS] Failed to send to client {client.Key}, removing.");
                        _clients.TryRemove(client.Key, out _);
                    }
                }
            }
        }
    }
}
