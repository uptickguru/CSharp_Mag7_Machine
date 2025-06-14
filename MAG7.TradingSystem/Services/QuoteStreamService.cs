using System.Text.Json;
using MAG7.TradingSystem.Streaming;

namespace MAG7.TradingSystem.Services
{
    public class QuoteStreamService
    {
        private readonly QuoteStreamHub _hub;

        public QuoteStreamService(QuoteStreamHub hub)
        {
            _hub = hub;
        }

        public async Task ForwardTickAsync(string rawJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(rawJson);
                if (doc.RootElement.TryGetProperty("data", out var data))
                {
                    await _hub.BroadcastAsync(new
                    {
                        type = "tick",
                        data = data
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FORWARD ERROR] {ex.Message}");
            }
        }
    }
}
