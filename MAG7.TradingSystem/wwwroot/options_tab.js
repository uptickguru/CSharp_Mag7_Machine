const OptionsTab = () => {
    const [symbols, setSymbols] = React.useState([]);
    const [selected, setSelected] = React.useState('');
    const [options, setOptions] = React.useState([]);

    React.useEffect(() => {
        fetch('/config/options.json')
            .then(res => res.json())
            .then(data => setSymbols(data.OptionSymbols || []));
    }, []);

    React.useEffect(() => {
        if (!selected) return;
        fetch(`/api/options/${selected}`)
            .then(res => res.json())
            .then(data => setOptions(data));
    }, [selected]);

    return (
        <div className="space-y-4">
            <select
                className="bg-gray-800 text-white px-4 py-2 rounded"
                value={selected}
                onChange={(e) => setSelected(e.target.value)}
            >
                <option value="">Select Symbol</option>
                {symbols.map((s, i) => (
                    <option key={i} value={s}>{s}</option>
                ))}
            </select>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {options.map((o, i) => (
                    <div key={i} className="card">
                        <div className="text-lg font-bold">{o.optionType} {o.strikePrice}</div>
                        <div>Δ {o.delta}, Γ {o.gamma}, Θ {o.theta}</div>
                        <div>Bid: {o.bid} / Ask: {o.ask} / IV: {o.impliedVolatility}</div>
                        <div>Prob ITM: {o.probabilityItm}, OI: {o.openInterest}</div>
                    </div>
                ))}
            </div>
        </div>
    );
};

ReactDOM.createRoot(document.getElementById('root')).render(<OptionsTab />);
