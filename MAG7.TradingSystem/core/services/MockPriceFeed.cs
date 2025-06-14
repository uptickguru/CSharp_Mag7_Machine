// Mock implementation of IPriceFeed
public class MockPriceFeed : IPriceFeed {
    public decimal GetPrice(string symbol) => 4200.00m;
}