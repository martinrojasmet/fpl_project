The Fantasy Premier League (FPL) Analytics Engine is an end-to-end data engineering pipeline designed to automate the extraction, integration, and modeling of multi-source football data. To address a key statistical gap in the official FPL API: historical expected goals (xG) and expected assists (xA) prior to 2023. The system automatically ingests match and player data from understat.com alongside official FPL endpoints. Containerized entirely using Docker and managed via DevContainers, the pipeline utilizes Apache Airflow to orchestrate daily ETL workflows, managing data extraction, raw staging, and schema harmonization within a PostgreSQL target database.

To solve complex entity resolution challenges stemming from mismatched player naming conventions and changing team affiliations across disparate sources, the integration layer pairs fuzzy string matching algorithms with Google AI Studio LLMs. This hybrid approach automates cross-source record linkage with high accuracy, eliminating manual mapping. Once integrated, dbt handles downstream data transformation,  modular data modeling, and optimized analytical views. The resulting schema feeds a baseline points prediction model, serving as a production-ready data foundation for future machine learning enhancements and web-facing dashboard deployment.

## Requirements

### 1. Docker
Have Docker installed and running.

### 2. Environment Setup
Create a `.env` file in the root directory of the project with the following parameters:

```env
# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_database_name

# Apache Airflow Credentials
AIRFLOW_USER=your_airflow_username
AIRFLOW_PASSWORD=your_airflow_password
AIRFLOW_SECRET_KEY=your_generated_secret_key

# External APIs (all free btw)
GOOGLE_AI_API_KEY=your_google_ai_studio_api_key
```

That is all, hope you enjoy it :)