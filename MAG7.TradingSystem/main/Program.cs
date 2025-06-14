#define OPTION_TEST

using System.Text.Json;
using Microsoft.Extensions.Configuration;
using MAG7.TradingSystem.Brokers.Tasty;
using MAG7.TradingSystem.Brokers.Alpaca;
using MAG7.TradingSystem.Data;
using MAG7.TradingSystem.Models;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("[MAG7] Starting MAG7 Trading System backend...");

        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        var authClient = new TastyTradeAuthClient(config);
        string sessionToken = await authClient.AuthenticateAsync();

        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddSignalR();
        builder.Services.AddSingleton<IConfiguration>(config);
        builder.Services.AddSingleton<TimescaleDbWriter>(sp =>
        {
            var connStr = config.GetConnectionString("Timescale")
                         ?? throw new Exception("Timescale connection string missing.");
            return new TimescaleDbWriter(connStr);
        });
        builder.Services.AddSingleton<TastyTradeQuoteClient>();
        builder.Services.AddHttpClient(); // IHttpClientFactory
        builder.Services.AddHttpClient<AlpacaHistoricalClient>();
        builder.Services.AddSingleton<AlpacaHistoricalClient>();

        var app = builder.Build();

#if OPTION_TEST
        Console.WriteLine("[MODE] Running in OPTION TEST mode...");

        app.MapGet("/api/options/{symbol}", async (string symbol, IHttpClientFactory httpFactory) =>
        {
            var sessionToken = System.IO.File.ReadAllText("tasty-session.token").Trim();
            var tastyHttp = httpFactory.CreateClient();
            var client = new TastyOptionClient(tastyHttp, sessionToken);

            var chain = await client.GetOptionChainRawAsync(symbol);
            if (chain == null)
                return Results.NotFound();

            return Results.Ok(chain.Value);
        });

        app.MapGet("/api/options-with-range/{symbol}", async (
            string symbol,
            IHttpClientFactory httpFactory,
            IConfiguration config
        ) =>
        {
            var sessionToken = System.IO.File.ReadAllText("tasty-session.token").Trim();

            var tastyHttp = httpFactory.CreateClient();
            var tastyClient = new TastyOptionClient(tastyHttp, sessionToken);
            var chain = await tastyClient.GetOptionChainRawAsync(symbol);
            if (chain == null)
                return Results.NotFound();

            var alpacaHttp = httpFactory.CreateClient();
            var alpacaClient = new AlpacaHistoricalClient(alpacaHttp, config);
            var candles = await alpacaClient.GetBarsAsync(symbol, 3);

            if (candles.Count == 0)
                return Results.Problem("No historical bars returned");

            var high = candles.Max(c => c.High);
            var low = candles.Min(c => c.Low);

            return Results.Ok(new
            {
                Symbol = symbol,
                High3Day = high,
                Low3Day = low,
                RawOptionChain = chain
            });
        });

#else
        Console.WriteLine("[MODE] Running in LIVE STREAMING mode...");

        string strategy = args.FirstOrDefault(a => a.StartsWith("--strategy="))?.Split('=')[1] ?? "default";
        string symbolsFile = Path.Combine("config", $"symbols.{strategy}.json");

        if (!File.Exists(symbolsFile))
        {
            Console.WriteLine($"[ERROR] Symbol file not found: {symbolsFile}");
            return;
        }

        Console.WriteLine($"[INFO] Loading watchlist from {symbolsFile}");

        var symbolConfig = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile(symbolsFile, optional: false)
            .Build();

        var symbols = symbolConfig.GetSection("Symbols")
                                  .GetChildren()
                                  .Select(s => s.Value)
                                  .Where(s => !string.IsNullOrWhiteSpace(s))
                                  .ToArray();

        Console.WriteLine($"[INFO] Streaming the following symbols ({symbols.Length} total):");
        foreach (var symbol in symbols)
            Console.WriteLine($"  → {symbol}");

        app.UseWebSockets();
        app.MapHub<QuoteStreamHub>("/stream");

        _ = Task.Run(async () =>
        {
            var quoteClient = app.Services.GetRequiredService<TastyTradeQuoteClient>();
            await quoteClient.StartAsync(sessionToken, async json =>
            {
                var hub = app.Services.GetRequiredService<IHubContext<QuoteStreamHub>>();
                await hub.Clients.All.SendAsync("tick", json);
            });
        });
#endif
        app.UseDefaultFiles();
        app.UseStaticFiles();
        app.Run();
    }
}
