using MAG7.TradingSystem.Models;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace MAG7.TradingSystem.Data
{
    public class TimescaleDbWriter
    {
        private readonly string _connectionString;

        public TimescaleDbWriter(string connectionString)
        {
            _connectionString = connectionString;
        }
        public async Task InsertRawQuoteJsonAsync(string symbol, string rawJson)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(
                "INSERT INTO tick_raw_data (timestamp, symbol, raw_json) VALUES (@ts, @sym, @json)",
                conn);

            cmd.Parameters.AddWithValue("ts", DateTime.UtcNow);
            cmd.Parameters.AddWithValue("sym", symbol);
            cmd.Parameters.AddWithValue("json", rawJson);

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task InsertQuoteTickAsync(QuoteTick tick)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(
                "INSERT INTO tick_data (timestamp, symbol, bid, ask, last, volume) VALUES (@ts, @sym, @bid, @ask, @last, @vol)",
                conn);

            cmd.Parameters.AddWithValue("ts", tick.Timestamp);
            cmd.Parameters.AddWithValue("sym", tick.Symbol);
            cmd.Parameters.AddWithValue("bid", tick.Bid);
            cmd.Parameters.AddWithValue("ask", tick.Ask);
            cmd.Parameters.AddWithValue("last", tick.Last);
            cmd.Parameters.AddWithValue("vol", tick.Volume);

            await cmd.ExecuteNonQueryAsync();
        }


        public async Task InsertTickAsync(string symbol, double bid, double ask, double last, long volume)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(
                "INSERT INTO tick_data (timestamp, symbol, bid, ask, last, volume) VALUES (@ts, @sym, @bid, @ask, @last, @vol)",
                conn);

            cmd.Parameters.AddWithValue("ts", DateTime.UtcNow);
            cmd.Parameters.AddWithValue("sym", symbol);
            cmd.Parameters.AddWithValue("bid", bid);
            cmd.Parameters.AddWithValue("ask", ask);
            cmd.Parameters.AddWithValue("last", last);
            cmd.Parameters.AddWithValue("vol", volume);

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<(string Symbol, double Last)> ReadLatestTickAsync(string symbol)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(
                "SELECT symbol, last FROM tick_data WHERE symbol = @sym ORDER BY timestamp DESC LIMIT 1",
                conn);
            cmd.Parameters.AddWithValue("sym", symbol);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return (
                    reader.GetString(0),
                    reader.GetDouble(1)
                );
            }

            throw new Exception($"No tick found for symbol {symbol}");
        }

        public async Task DeleteTestTicksAsync(string symbol)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand("DELETE FROM tick_data WHERE symbol = @sym", conn);
            cmd.Parameters.AddWithValue("sym", symbol);

            await cmd.ExecuteNonQueryAsync();
        }
    }
}
