{% docs __overview__ %}

# Fuel Weather ELT

Weather (Open-Meteo) and weekly fuel prices (ANP, through the Base dos Dados mirror) for five agricultural states (SP, GO, MG, MS, PR), used to test whether rainfall helps explain ethanol and gasoline price changes.

**Layers**
- `bronze`: raw weather loaded by Python, plus the `bronze_locations` seed.
- `silver`: cleaned weather and weekly prices by state and product.
- `gold`: dimensions, facts and the rain vs price correlation (`gold_corr_rain_price`).

**Result:** no combination of state, product and rainfall lag (0 to 8 weeks) is significant after seasonal adjustment and a Bonferroni correction. See the [repository README](https://github.com/carlosMartinez93/Fuel_Weather_ELT) for the limitations.

Use the blue graph icon at the bottom right to explore the lineage.

{% enddocs %}