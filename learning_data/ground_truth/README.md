# Ground Truth Mappings

One JSON file per sample spreadsheet. Filename = sample file stem (no extension).

Schema:
```json
{
  "<sheet_name>::<segment_index>": {
    "mappings": {
      "<raw header>": "<target_column>",
      ...
    }
  }
}
```

When building new ground-truth files, first run the sample through the CURRENT engine interactively, confirm the mapping, and copy the accepted dict into the JSON file.
