using System.Net.WebSockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using MAG7.TradingSystem.Brokers.Tasty;
using MAG7.TradingSystem.Data;
using MAG7.TradingSystem.Services;
using MAG7.TradingSystem.Streaming;


var builder = WebApplication.CreateBuilder(args);

// Load configuration
builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "config", "symbols.default.json"), optional: false);

builder.Services.AddSingleton<TastyTradeAuthClient>();
//builder.Services.AddSingleton<TimescaleDbWriter>();
builder.Services.AddSingleton<QuoteStreamHub>();
builder.Services.AddSingleton<QuoteStreamService>();
builder.Services.AddSingleton<TimescaleDbWriter>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var connStr = config.GetConnectionString("Timescale") ?? throw new Exception("Timescale connection string missing.");
    return new TimescaleDbWriter(connStr);
});


var app = builder.Build();

app.UseWebSockets();

app.Map("/stream", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var socket = await context.WebSockets.AcceptWebSocketAsync();
        var hub = app.Services.GetRequiredService<QuoteStreamHub>();
        await hub.HandleClientAsync(context, socket);
    }
    else
    {
        context.Response.StatusCode = 400;
    }
});

Console.WriteLine("[MAG7] Starting MAG7 Trading System backend...");

// Trigger your auth + TastyTrade stream if desired:
var config = app.Services.GetRequiredService<IConfiguration>();
var tasty = app.Services.GetRequiredService<TastyTradeAuthClient>();
var db = app.Services.GetRequiredService<TimescaleDbWriter>();
var streamService = app.Services.GetRequiredService<QuoteStreamService>();

var sessionToken = await tasty.AuthenticateAsync();
string[] watchlist = config.GetSection("Symbols").GetChildren()
    .Select(s => s.Value)
    .Where(s => !string.IsNullOrWhiteSpace(s))
    .Select(s => s!)
    .ToArray();

Console.WriteLine($"[INFO] Streaming the following symbols ({watchlist.Length} total):");
foreach (var symbol in watchlist)
    Console.WriteLine($"  → {symbol}");

var quoteClient = new TastyTradeQuoteClient(config["TastyTrade:WebSocketUrl"]!, watchlist, db);

// Stream and forward ticks
_ = Task.Run(async () =>
{
    await quoteClient.StartAsync(sessionToken, async (tickJson) =>
    {
        await streamService.ForwardTickAsync(tickJson);
    });
});

await app.RunAsync();
