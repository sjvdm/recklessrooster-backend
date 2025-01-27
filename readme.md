Backend services for recklessrooster.com - finding crime-prone animals - once!

**This readme is limited as this is still a work in progress. The code should be self-explanatory at this stage, and I plan to add proper documentation in due time**

## GBIF Public BQ database

GBIF occurences are hosted and accessible publicly on BQ.

Grab a sample with: 

```
CREATE TABLE `your_project.recklessroosters.gbif_src` AS
SELECT *
FROM `bigquery-public-data.gbif.occurrences`
TABLESAMPLE SYSTEM (2 PERCENT);
```
