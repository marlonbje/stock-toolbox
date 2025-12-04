# stock-toolbox

![Python](https://img.shields.io/badge/python-3.10+-blue?logo=python)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange?logo=jupyter)
![License](https://img.shields.io/badge/license-MIT-green)

Ein kompaktes Python-Toolkit für Aktienanalyse, Portfolio-Auswertung und Risikomanagement – alles direkt in Jupyter-Notebooks.

---

## 📂 Inhalte

- **`beta.ipynb`**  
  Berechnung und Visualisierung des Aktien‑Betas relativ zu einem Referenzindex.

- **`diversification_dashboard.ipynb`**  
  Analyse der Diversifikation eines Portfolios: Sektoren, Regionen, Einzelpositionsrisiko.

- **`fundamentals_dashboard.ipynb`**  
  Fundamentalanalyse von Einzelaktien: Bewertung, Profitabilität, Verschuldung.

- **`portfolio_dashboard.ipynb`**  
  Portfolio-Übersicht: Performance, Rendite-/Risikokennzahlen, Volatilität, Drawdown.

- **`research_filter.ipynb`**  
  Screening-/Filter-Notebook zur Auswahl von Aktien nach quantitativen Kriterien.

- **`value_at_risk.ipynb`**  
  Berechnung des Value at Risk (VaR) eines Portfolios (historisch oder parametrisch).

- **`worldstocks.txt`**  
  Tickersymbolliste für weltweite Aktien, genutzt in den Notebooks.

- **`yfdownloader.py`**  
  Hilfsskript zum Laden von Kursdaten über die `yfinance`-API und zum Speichern lokal.

---

## ⚙️ Installation

1. **Repository klonen**
   ```bash
   git clone https://github.com/<dein-user>/stock-toolbox.git
   cd stock-toolbox

	2.	Virtuelle Umgebung einrichten und Abhängigkeiten installieren

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

Typische Pakete: pandas, numpy, plotly, yfinance

⸻

🚀 Verwendung
	1.	Jupyter Notebook starten

jupyter notebook


	2.	Gewünschtes Notebook öffnen (z. B. portfolio_dashboard.ipynb).
	3.	Parameter wie Tickerliste, Zeitraum oder Startkapital anpassen und alle Zellen ausführen.

⸻

⚠️ Haftungsausschluss

Alle Notebooks dienen ausschließlich Bildungs- und Demonstrationszwecken und stellen keine Anlageberatung dar. Jede Investitionsentscheidung erfolgt auf eigenes Risiko.
