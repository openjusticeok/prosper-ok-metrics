# Findings

## Profile

- `fiscal_year` on `profile` table represents the fiscal year of release date
- `profile` rows were queried by release dates within fiscal years 2024 and 2025 
- `doc_num` is not distinct
  - Some duplicate `doc_num` due to separate periods, but some have same `admit_date` and different `release_date`
  - 10 rows (5 people) have same `admit_date` with different `release_date`
  - This causes a many-to-many join when attempting to add to `sentence`
  - Some duplicate `release_date` are in different months
  - All duplicate `release_date` are in same fiscal year

## Sentences

- Not all `fiscal_year` match between joined `profile` and `sentence`

## Alias

- Lots of aliases don't link to a profile and vice versa. 



