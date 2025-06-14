// File: Brokers/Alpaca/AlpacaHistoricalClient.cs

// v1.1.0 - Returns parsed high/low/close values for last N daily bars

using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace MAG7.TradingSystem.Brokers.Alpaca
{
    public class AlpacaHistoricalClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        private readonly string _apiSecret;

        public AlpacaHistoricalClient(HttpClient httpClient, IConfiguration config)
        {
            _httpClient = httpClient;
            _apiKey = config["Alpaca:Key"];
            _apiSecret = config["Alpaca:Secret"];

            _httpClient.DefaultRequestHeaders.Add("APCA-API-KEY-ID", _apiKey);
            _httpClient.DefaultRequestHeaders.Add("APCA-API-SECRET-KEY", _apiSecret);
        }

        public async Task<List<DailyCandle>> GetBarsAsync(string symbol, int days = 3)
        {
            var url = $"https://data.alpaca.markets/v2/stocks/{symbol}/bars?timeframe=1Day&limit={days}";
            var response = await _httpClient.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                throw new HttpRequestException($"Failed to fetch historical data for {symbol}: {response.StatusCode}");

            var stream = await response.Content.ReadAsStreamAsync();
            var doc = await JsonDocument.ParseAsync(stream);
            var list = new List<DailyCandle>();

            foreach (var bar in doc.RootElement.GetProperty("bars").EnumerateArray())
            {
                list.Add(new DailyCandle
                {
                    Time = bar.GetProperty("t").GetDateTime(),
                    Open = bar.GetProperty("o").GetDouble(),
                    High = bar.GetProperty("h").GetDouble(),
                    Low = bar.GetProperty("l").GetDouble(),
                    Close = bar.GetProperty("c").GetDouble(),
                    Volume = bar.GetProperty("v").GetInt64()
                });
            }

            return list;
        }
    }

    public class DailyCandle
    {
        public DateTime Time { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long Volume { get; set; }
    }
}
