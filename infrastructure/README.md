# Microsoft Fabric Infrastructure

Microsoft Fabric is now the active cloud target for this project. The previous AWS Terraform placeholders are retired; provisioning is performed through Fabric workspace items, Fabric Git integration, deployment pipelines, or Fabric REST APIs depending on the tenant setup.

## Required Fabric Items

Create these items in the same workspace for a development deployment:

| Item type | Name |
| --- | --- |
| Lakehouse | `weather_energy_lakehouse` |
| Environment | `weather_energy_env` |
| Notebook | `01_ingest_api_to_bronze` |
| Notebook | `02_bronze_to_silver` |
| Notebook | `03_build_gold_tables` |
| Notebook | `04_data_quality_checks` |
| Data pipeline | `weather_energy_demand_pipeline` |

## Security

Store these secrets outside the repo:

- `OPENWEATHER_API_KEY`
- `NATIONAL_GRID_API_TOKEN`

Use one of:

- Secure Fabric pipeline parameters
- Fabric connections
- Azure Key Vault-backed secret access from notebooks

## Promotion

Use separate Fabric workspaces for dev, test, and production when this moves beyond a portfolio or prototype pipeline. Promote the Lakehouse, Environment, notebooks, SQL views, and Data Factory pipeline together so table names and schedules stay consistent.
