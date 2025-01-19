Backend services for recklessrooster.com.

## GBIF Public BQ database

GBIF occurences are hosted and accessible publicly on BQ.

Grab a sample with: 

```
CREATE TABLE `your_project.recklessroosters.gbif_src` AS
SELECT *
FROM `bigquery-public-data.gbif.occurrences`
TABLESAMPLE SYSTEM (2 PERCENT);
```
