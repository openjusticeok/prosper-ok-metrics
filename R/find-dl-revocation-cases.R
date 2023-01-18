#'
#' Trying to retrieve all instances where someone had their DL revoked by DPS
#'

library(ojodb)
library(tidyverse)

# Approach 1 -- searching minutes  ===============
# Note: Abandoned this approach due to messy minute data, but I should revisit it
# TODO: revisit this approach if time allows

# # Using ojo_search_minutes:
# min_ <- ojo_search_minutes("suspend | suspension") |>
#   collect()
#
# min <- min_ |>
#   filter(
#     code %in% c("ABST", "NOSPSe", "NOWS", "NOSRT", "TEXT", "CNOTE", "NOSUS", "NO", "RULE8", "STAY", "WRCI"),
#     str_detect(description, "(DPS|DEPARTMENT OF PUBLIC SAFETY|D\\.P\\.S)"),
#     !str_detect(description, "(WITHDRAW|ERROR|RETURN|UNABLE|REVOKE|LIFT|RECALL|NOT PROCESSED|PENDING|SUSPENSION RELEASE|DEFENDANT APPEARS)")
#   )
#
# min |>
#   filter(date > ymd("2016-01-01")) |>
#   count(month = floor_date(date, "months")) |>
#   ggplot(aes(x = month, y = n)) +
#   geom_point() +
#   geom_line()


# Using minute data pulled with ojo_tbl("minute")
# Stored locally so I don't have to wait for the ~30 minute pull again:
# files <- list.files("/home/andrew/Documents/Data/oscn/minutes/",
#                     pattern = "*.rds",
#                     full.names = TRUE)
#
# min_all_ <- lapply(files, read_rds) |>
#   bind_rows()



# Approach 2 -- Misdemeanor drug cases ========================

current_year <- year(today())

# Collecting all CM cases filed 2021 - present
cm_ <- ojo_crim_cases(case_types = "CM",
                      file_years = 2019:current_year) |>
  collect()

# cf_ <- ojo_crim_cases(case_types = "CF",
#                       file_years = 2019:current_year) |>
#   collect()

# Search string for all drug-related charges
drugs <- "CDS|C\\.D\\.S|DRUG|OXY|HUFF|AMPHET|ZOLOL|ZOLAM|HYDROC|CODEIN|PRECURS|XANAX|MORPH|METERDI|ZEPAM|LORAZ|VALIU|EPHED|SUB|COCA|PSEUDO| CS|CS | CD|CD |PRESCRIP|NARC|METH|C\\.D\\.|HEROIN|ANHYD|AMMONIA|OPIUM|LORTAB|PARAPHERNALIA|MARIJUANA|MARIHUANA|MJ"

# Adding variables for whether the charge was filed was pre- / post- November 2021, and whether the charge description indicates a drug charge.
cm_charges <- cm_ |>
  mutate(
    file_year = year(date_filed),
    quarter_fy = lubridate::quarter(date_filed,
                                    fiscal_start = 7,
                                    type = "year.quarter"),
    quarter_fy_start_date = lubridate::quarter(date_filed,
                                          fiscal_start = 7,
                                          type = "date_first"),
    pre_november = if_else(date_filed < ymd("2021-11-01"), TRUE, FALSE),
    drug_charge = if_else(grepl(drugs, count_as_filed), TRUE, FALSE)
  ) |>
  filter(file_year >= 2019) # To remove data errors (e.g. cases filed in the year 1400, 3200, etc)

# Grouping / summarizing the charges by case
cm_cases <- cm_charges |>
  group_by(case_number, district) |>
  summarize(
    file_date = unique(date_filed),
    file_year = unique(file_year),
    quarter_fy = unique(quarter_fy),
    quarter_fy_start_date = unique(quarter_fy_start_date),
    pre_november = if_else(sum(pre_november, na.rm = T) > 0, TRUE, FALSE),
    n_charges = n(),
    drug_charge_present = if_else(sum(drug_charge, na.rm = T) > 0, TRUE, FALSE),
    all_drugs = if_else(sum(drug_charge, na.rm = T) == n_charges, TRUE, FALSE),
    charges_list = paste(count_as_filed, collapse = "; ")
  ) |>
  ungroup() |>
  mutate(
    file_month = floor_date(file_date, "months")
  )


# Checks

# Case filings per month over time -- are there potential gaps / etc?
cm_cases |>
  count(file_month) |>
  filter(file_month < ymd("2022-10-01")) |>
  ggplot(aes(x = file_month,
             y = n)) +
  geom_text(aes(label = file_month)) +
  geom_line() +
  lims(y = c(0, NA)) +
  labs(title = "Total CM filings per month statewide",
       x = "File month",
       y = "N filed")

cm_cases |>
  count(quarter_fy, all_drugs) |>
  ggplot(aes(x = quarter_fy,
             y = n,
             fill = all_drugs
             )
         ) +
  geom_col(position = "dodge") +
  geom_text(aes(label = quarter_fy),
            nudge_y = 100) +
  facet_wrap(~all_drugs) +
  labs(title = "Total CM per month statewide",
       x = "Fiscal year / quarter filed",
       y = "N filed",
       fill = "All drug charges?")


cm_cases |>
  # filter(all_drugs) |>
  group_by(quarter_fy, all_drugs) |>
  summarize(
    n = n()
  )

cm_charges |>
  filter(drug_charge) |>
  count(count_as_filed) |>
  arrange(desc(n)) |>
  view()

# most common charges
cm_cases |>
  group_by(pre_november) |>
  summarize(
    n_cases_with_drug_charges = sum(drug_charge_present, na.rm = T)
  )

# county coverage
cm_cases |>
  group_by(district, file_year) |>
  count() |>
  ggplot(aes(x = reorder(district, n), y = n)) +
  geom_col() +
  coord_flip() +
  facet_wrap(~file_year) +
  labs(
    title = "Total Misdemeanor cases filed Statewide",
    subtitle = "2021 & 2022",
    x = "",
    y = "CM Cases Filed"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::comma)

# cm_cases |>
#   filter(!pre_november,
#          all_drugs) |>
#   view()

cm_cases |>
  filter(
    file_month < ymd("2022-10-01"),
    # all_drugs == TRUE
  ) |>
  group_by(file_month, all_drugs) |>
  summarize(n = n()) |>
  ggplot(aes(x = file_month, y = n)) +
  geom_line() +
  scale_y_continuous(limits = c(0, NA)) +
  labs(title = "CM cases w/ only drug charges",
       subtitle = "Statewide, January 2019- August 2022",
       x = "Month of original case filing",
       y = "Number of CM cases w/ only drug charges",
  ) +
  facet_wrap(~all_drugs) +
  scale_y_continuous(labels = scales::comma)


# Export data if everything looks good ===============

export_data <- cm_cases |>
  filter(file_month < ymd("2022-10-01")) |>
  group_by(quarter_fy) |>
  summarize(
    n_cm_cases = n(),
    n_cm_cases_all_drugs = sum(all_drugs, na.rm = T),
    n_cm_cases_at_least_one_drugs = sum(drug_charge_present, na.rm = T)
  )


