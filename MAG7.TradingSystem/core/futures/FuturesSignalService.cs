// Futures signal logic
public class FuturesSignalService : IFuturesSignalService {
    private readonly IPriceFeed _priceFeed;
    public FuturesSignalService(IPriceFeed priceFeed) { _priceFeed = priceFeed; }
    public void EvaluateSignals() { var price = _priceFeed.GetPrice("ES"); /* logic */ }
}