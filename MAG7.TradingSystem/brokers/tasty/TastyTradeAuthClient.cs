using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace MAG7.TradingSystem.Brokers.Tasty
{
    public class TastyTradeAuthClient
    {
        private readonly HttpClient _http;
        private readonly string _baseUrl;
        private readonly string _username;
        private readonly string _password;
        private readonly string _tokenFile;

        private string? _sessionToken;
        public string? SessionToken => _sessionToken;

        public TastyTradeAuthClient(IConfiguration config)
        {
            _baseUrl = config["TastyTrade:BaseUrl"] ?? throw new ArgumentException("BaseUrl is missing in configuration.");
            _username = config["TastyTrade:Username"] ?? throw new ArgumentException("Username is missing in configuration.");
            _password = config["TastyTrade:Password"] ?? throw new ArgumentException("Password is missing in configuration.");
            _tokenFile = config["TastyTrade:SessionTokenPath"] ?? "tasty-session.token";

            _http = new HttpClient { BaseAddress = new Uri(_baseUrl) };
            _http.DefaultRequestHeaders.UserAgent.ParseAdd("grok-client/1.0");
        }

        public async Task<string> AuthenticateAsync()
        {
            Console.WriteLine($"[AUTH] Base URL: {_baseUrl}");
            Console.WriteLine($"[AUTH] Username: {_username}");
            Console.WriteLine($"[AUTH] Token file: {_tokenFile}");

            if (File.Exists(_tokenFile))
            {
                var cached = await File.ReadAllTextAsync(_tokenFile);
                if (!string.IsNullOrWhiteSpace(cached))
                {
                    Console.WriteLine("[AUTH] Using cached session token.");
                    _sessionToken = cached;

                    if (await TryGetQuoteToken(_sessionToken))
                        return _sessionToken;

                    Console.WriteLine("[AUTH] Cached token rejected — requesting new token.");
                }
            }

            // Compose login payload
            var payload = new Dictionary<string, object>
            {
                { "login", _username },
                { "password", _password },
                { "remember-me", true }
            };

            var loginResponse = await _http.PostAsJsonAsync("/sessions", payload);

            if (!loginResponse.IsSuccessStatusCode)
            {
                var err = await loginResponse.Content.ReadAsStringAsync();
                Console.WriteLine($"[AUTH ERROR] Status: {(int)loginResponse.StatusCode} {loginResponse.StatusCode}");
                Console.WriteLine($"[AUTH ERROR] Response: {err}");
                throw new HttpRequestException("TastyTrade login failed");
            }

            var json = await loginResponse.Content.ReadAsStringAsync();
            var doc = JsonDocument.Parse(json);
            _sessionToken = doc.RootElement.GetProperty("data").GetProperty("session-token").GetString();

            if (_sessionToken is null)
                throw new Exception("TastyTrade returned null session token.");

            Console.WriteLine("[AUTH] Logged in successfully. Session token acquired.");
            await File.WriteAllTextAsync(_tokenFile, _sessionToken);

            if (!await TryGetQuoteToken(_sessionToken))
                throw new Exception("Failed to acquire quote token after login.");

            return _sessionToken;
        }

        private async Task<bool> TryGetQuoteToken(string token)
        {
            using var quoteClient = new HttpClient();
            quoteClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(token);
            quoteClient.DefaultRequestHeaders.UserAgent.ParseAdd("grok-client/1.0");

            var quoteResponse = await quoteClient.GetAsync("https://api.tastytrade.com/api-quote-tokens");
            if (!quoteResponse.IsSuccessStatusCode)
            {
                Console.WriteLine($"[AUTH] Failed to get quote token with session. Status: {quoteResponse.StatusCode}");
                return false;
            }

            var json = await quoteResponse.Content.ReadAsStringAsync();
            var doc = JsonDocument.Parse(json);
            var quoteToken = doc.RootElement.GetProperty("data").GetProperty("token").GetString();
            Console.WriteLine($"[AUTH] API quote token acquired: {quoteToken}");
            return true;
        }
    }
}
