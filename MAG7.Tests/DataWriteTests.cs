using Microsoft.VisualStudio.TestTools.UnitTesting;
using MAG7.TradingSystem.Data;
using System.Threading.Tasks;

namespace MAG7.Tests
{
    [TestClass]
    public class DataWriteTests
    {
        private const string ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=your_actual_password;Database=mag7";

        [TestMethod]
        public async Task InsertTickAsync_Then_ReadsBackSuccessfully()
        {
            // Arrange
            var writer = new TimescaleDbWriter(ConnectionString);
            string symbol = "TEST123";
            double bid = 100.01;
            double ask = 100.05;
            double last = 100.03;
            long volume = 12345;

            // Act
            await writer.InsertTickAsync(symbol, bid, ask, last, volume);
            await Task.Delay(100); // brief delay in case of slow write
            var result = await writer.ReadLatestTickAsync(symbol);

            // Assert
            Assert.AreEqual(symbol, result.Symbol);
            Assert.AreEqual(last, result.Last, 0.0001); // with precision tolerance
        }
    }
}
