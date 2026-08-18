# NGED Resource ID Lookup

This pipeline uses the National Grid Electricity Distribution (NGED) Connected Data Portal and CKAN's `datastore_search`. It needs a resource ID UUID for one licence-area resource in the **Live Data** dataset.

Option A (UI).
1. Sign in to the NGED Connected Data Portal if the selected resource requires it.
2. Open the **Live Data** dataset.
3. Open the specific licence-area resource.
4. Copy the resource ID UUID shown on the resource page or in the URL.

Option B (API).
1. Call `package_show` to list resources for the dataset.
2. Find the resource UUID at `result.resources[].id`.
3. Set that UUID in `ingestion/energy/config.yaml` as `resource_id`.

Example (API).
```bash
curl -s \
  -H "Authorization: $NATIONAL_GRID_API_TOKEN" \
  "https://connecteddata.nationalgrid.co.uk/api/3/action/package_show?id=live-data"
```

Example (small diagnostic fetch only).
```bash
curl -s \
  -H "Authorization: $NATIONAL_GRID_API_TOKEN" \
  "https://connecteddata.nationalgrid.co.uk/api/3/action/datastore_search?resource_id=<resource_uuid>&limit=5&sort=_id%20asc"
```

The pipeline does not treat that diagnostic limit as a completeness boundary. It follows deterministic pages until the total reported by the first page is reached, then stores the combined snapshot with pagination evidence. Configure `page_size` and the explicit `max_records` safety bound in `config.yaml`.

Some portal endpoints use `Ocp-Apim-Subscription-Key` instead of `Authorization`. If your token requires a different header, update `api_key_header` in `ingestion/energy/config.yaml` accordingly.
