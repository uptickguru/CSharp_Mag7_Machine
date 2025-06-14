using System;

namespace MAG7.TradingSystem.Models
{
    public class QuoteTick
    {
        public string Symbol { get; set; } = "";
        public double Bid { get; set; }
        public double Ask { get; set; }
        public double Last { get; set; }
        public long Volume { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
