// File: Brokers/Tasty/TastyOptionClient.cs

// v1.1.0 - Updated to use nested option chains, inject token, parse structure
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;

namespace MAG7.TradingSystem.Brokers.Tasty;

public class TastyOptionClient
{
    private readonly HttpClient _http;

    public TastyOptionClient(HttpClient httpClient, string token)
    {
        _http = httpClient;
        _http.BaseAddress = new Uri("https://api.tastytrade.com/");
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    public async Task<JsonElement?> GetOptionChainRawAsync(string symbol)
    {
        var res = await _http.GetAsync($"option-chains/{symbol}/nested");

        if (!res.IsSuccessStatusCode)
            return null;

        var stream = await res.Content.ReadAsStreamAsync();
        var doc = await JsonDocument.ParseAsync(stream);
        return doc.RootElement.GetProperty("data");
    }
}
