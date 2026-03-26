# GFSC Snow Probability Tools

Verktøy for nedlasting og prosessering av Gap-filled Fractional Snow Cover (GFSC) data fra Copernicus Land Monitoring Service (CLMS) for beregning av snøsannsynlighet.

## Hva er GFSC?

[Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) er et daglig snødekkeprodukt med 60m oppløsning som kombinerer Sentinel-1 (radar) og Sentinel-2 (optisk) data for robust snødeteksjon med gap-filling.

## Innhold

| Fil | Beskrivelse |
|-----|-------------|
| `gfsc_data_downloader.py` | Last ned GFSC-data fra WEkEO og S3 (inkl. reprosesserte år) |
| `gfsc_snow_probability_processor.py` | Beregn snøsannsynlighet fra nedlastede data |

## Hurtigstart

### 1. Installer avhengigheter

```bash
pip install rasterio numpy pandas matplotlib seaborn hda boto3 retry tqdm geopandas shapely pyproj pyogrio python-dotenv
```

### 2. Last ned data

Kopier `.env.example` til `.env` og fyll inn dine WEkEO-credentials, og kjør:

```bash
python gfsc_data_downloader.py
```

### 3. Prosesser data

```bash
python gfsc_snow_probability_processor.py
```

## Dokumentasjon

Se [Gap-filled Fractional Snow Cover](https://land.copernicus.eu/en/products/snow/high-resolution-gap-filled-fractional-snow-cover) for detaljer om GFSC-produktet.

## Credentials

- **WEkEO** (data som ikke er reprosessert på S3): Registrer deg på [WEkEO](https://wekeo.copernicus.eu/register/), legg credentials i `.env` (se `.env.example`)
- **S3** (2025-data og reprosesserte år som 2017-2018): Ingen registrering nødvendig

## Lisens

MIT License - se [LICENSE](LICENSE)

Nedlastingsskriptet er basert på [eea/clms-hrsi-api-client-python](https://github.com/eea/clms-hrsi-api-client-python).

