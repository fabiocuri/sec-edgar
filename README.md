# SEC EDGAR

Platform: Google Cloud

Project name: OECD AI Policy Observatory

Virtual machine: vm-sec-edgar

Bigquery: sec_edgar

GitHub: https://github.com/oecd-ai-org/sec

Frequency: Monthly.

Methodology: https://oecd.ai/en/sec

Note: If a request limit message is sent, create a new free account in SEC EDGAR and replace the API key in Secret Manager.

General flow:

- Retrieve the list of 10K and 20F filings for a given year.
- Download the HTML of these filings.
- Parse these HTMLs.
- Create final dataframes that can be used for visualisations.

Output: 

| Type     | Name      | Visualisations |
|----------|-----------|---------|
| BigQuery | CHART_EVOLUTION_OF_US_REGISTERED_AI_PUBLIC_COMPANIES_PER_COUNTRY | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/SEC_10K_20F_TIME_SERIES/Tableaudebord1) [OECD.AI](https://oecd.ai/en/dashboards/countries/UnitedStates) |
| BigQuery | CHART_TOP_CONCEPTS_IN_AI_PUBLIC_COMPANIES_BUSINESS_DESCRIPTIONS          | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/SEC_10K_20F_KEYWORDS/Tableaudebord1) [OECD.AI](https://oecd.ai/en/dashboards/countries/UnitedStates)         |
| BigQuery | CHART_AI_PUBLIC_COMPANIES_PER_US_STATE          | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/SEC_10K_USSTATES/Tableaudebord1) [OECD.AI](https://oecd.ai/en/dashboards/countries/UnitedStates)         |
