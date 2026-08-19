# NGED Live Data licence-area resources

This pipeline uses CKAN `datastore_search` against National Grid Electricity Distribution's **Live Data** dataset. The dataset is split by licence area.

| `source_area` | Licence area | Resource ID | Project weather proxy |
| --- | --- | --- | --- |
| `east_midlands` | East Midlands | `92d3431c-15d7-4aa6-ad34-2335596a026c` | `Nottingham,GB` |
| `south_wales` | South Wales | `38b81427-a2df-42f2-befa-4d6fe9b54c98` | `Cardiff,GB` |
| `south_west` | South West | `85aaa199-15df-40ec-845f-6c61cbedc20f` | `Bristol,GB` |
| `west_midlands` | West Midlands | `1c3447df-37d7-4fb4-9f99-0e2a0d691dbe` | `Birmingham,GB` |

The resource IDs come from the NGED Live Data dataset. The city choices are project-owned representative proxies, not claims that one city describes weather across the entire licence area.

The authoritative machine-readable binding is `data-contracts/source_areas.json`. Local and Fabric ingestion reject a resource/city combination that does not match its configured `source_area` before making a network request.
