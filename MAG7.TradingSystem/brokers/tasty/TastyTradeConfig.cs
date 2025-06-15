// v1.0.1 - Warning-safe TastyTrade config class
namespace MAG7.TradingSystem.Brokers.Tasty
{
    public class TastyTradeConfig
    {
        public required string BaseUrl { get; set; }
        public required string Username { get; set; }
        public required string Password { get; set; }
        public required string TokenFile { get; set; }
    }
}
