# Resource ID Lookup (National Grid ESO / Connected Data Portal)

This pipeline uses CKAN's `datastore_search` and needs a dataset **resource ID** (a UUID) for the energy demand dataset.

Option A (UI).
1. Sign in to the National Grid ESO Connected Data Portal.
2. Open the dataset you want (for example, a demand dataset).
3. Open the specific resource (data file or API table).
4. Copy the Resource ID UUID shown on the resource page or in the URL.

Option B (API).
1. Call `package_show` to list resources for the dataset.
2. Find the resource UUID at `result.resources[].id`.
3. Set that UUID in `ingestion/energy/config.yaml` as `resource_id`.

Example (API).
```bash
curl -s \
  -H "Authorization: $NATIONAL_GRID_API_TOKEN" \
  "https://connecteddata.nationalgrid.co.uk/api/3/action/package_show?id=<dataset_id>"
```

Example (data fetch).
```bash
curl -s \
  -H "Authorization: $NATIONAL_GRID_API_TOKEN" \
  "https://connecteddata.nationalgrid.co.uk/api/3/action/datastore_search?resource_id=<resource_uuid>&limit=5"
```

Note.
Some older endpoints use `Ocp-Apim-Subscription-Key` instead of `Authorization`. If your token requires a different header, update `api_key_header` in `ingestion/energy/config.yaml` accordingly.
