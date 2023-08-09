library(ojodb)
library(here)
library(readr)
library(stringr)
library(ggplot2)
library(gt)
library(lubridate)
library(tidyr)
library(purrr)
library(extrafont)

ojodb <- ojo_connect()

# Pulling the number of suspended sentences for CMs / CFs

start_date <- ymd("2001-01-01")
end_date <- ymd("2023-01-01")

data_case <- ojo_tbl("case") |>
  select(
    id,
    case_number,
    district,
    case_type,
    date_filed,
    date_closed,
    created_at,
    updated_at,
    status
    ) |>
  filter(
    case_type %in% c("CM", "CF"),
    date_filed >= start_date,
    date_filed < end_date
  ) |>
  # We don't need the charges for anything I don't think
  # left_join(
  #   ojo_tbl("count", .con = ojodb),
  #   by = c("id" = "case_id"),
  #   suffix = c("", "_count")
  # ) |>
  ojo_collect()

# write_csv(data_case, here("data/cm_cf_2001_2022_no_charges.csv"))
# df_case_ <- read_csv(here("data/cm_cf_2001_2022.csv"))
df_case <- read_csv(here("data/cm_cf_2001_2022_no_charges.csv"))
# df_case <- data_case

#check
df_ids <- df_case |>
  distinct(id)

############ Pulling relevant minute tables
# We first look for any mention of suspended sentences in the description column.
# It appears that revocation or application for revocation of a suspended sentence is not uncommon and might be worth noting.
# If we want to include cases that result in a revocation of the suspended sentence or want to do further analysis with that on what might happen.
data_minutes <- ojo_tbl("minute") |>
  filter(date >= start_date,
         date < end_date) |>
   select(id,
          case_id,
          date,
          code,
          description,
          count,
          amount) |>
  # filter(str_detect(description, "(?i)^\\ssuspended sentence")) |>
  # filter(!str_detect(description, "(?i)\\brevok(e|ed|ing)\\b")) |>
  # # Trying some slightly more broad regex just to make sure we didn't leave anything on the table:
  filter(str_detect(description, "(?i)suspend"),
         str_detect(description, "(?i)sentence")) |>
  ojo_collect()

# write_csv(data_minutes, here("data/min_2001_2022.csv"))
data_minutes <- read_csv(here("data/min_2001_2022.csv"))
df_min <- data_minutes

# Gonna look at each case and see what minute codes are present,
# looking for one that's always there / consistent
drew_mins_check <- df_min |>
  group_by(case_id) |>
  summarize(
    n_mins = n(),
    list_codes = paste0(code, collapse = ", "),
    list_descs = paste0(description, collapse = ";; ")
  )

# Andrew thoughts: =============================================================
# It looks like the cleanest possible way to do this would be to only look at
# the CONVICTED / J&S minutes, because they ~should~ say "...suspended sentence" somewhere
# in the description, and that would reflect when the suspended sentence was issued.
# However, I'm noticing a ton of cases where the other minutes and the actual pdf court documents, etc.
# show a suspended sentence, but the CONVICTED / J&S minute doesn't have it. That makes me
# think we should also be looking at the "O", "AREV", "MO", etc. minutes too, even
# though a lot of those are orders / motions to revoke a suspended sentence. We
# can think of that like "how many cases include a minute indicating that a suspended
# sentence was issued at some point?"

# So, instead of trying to find every minute where someone was being given a suspended sentence,
# I'm going to look for every case with a minute indicating a suspended sentence was
# EVER ISSUED. That should give us a better "upper bound" estimate.

# All minutes related to sus sentences:
df_mins_final <- df_min |>
  filter(!is.na(case_id))

# Gonna leave these in, since I'm looking for any minute indicting a sus sentence
#111058
# df_min <- data_minutes |>
#   filter(
#     !str_detect(description, "(?i)dismiss|(?i)\\brevok(e|ed|ing)\\b|(?i)\\brevoc(atio|aton|ation)\\b|(?i)torevoke|revocke"),
#   )

# Summarizing the minutes so that we can more easily join them onto the case data
min_summary <- df_mins_final |>
  group_by(case_id) |>
  summarize(
    n_sus_mins = n(),
    list_sus_min_codes = paste0(code, collapse = "; "),
    list_sus_min_descs = paste0(description, collapse = "; "),
  )

# df_min("description") |>
#   inner_join(
#     oscn_cases,
#     by = c("case_id" = "id"))
#
# join_case_min <- min_summary |>
#   inner_join(
#     df_case |>
#       distinct(),
#       by = c("case_id" = "id"),
#     copy = TRUE
#     ) |>
#   mutate(file_year = lubridate::year(file_date))
#
# fijoin_case_minnal |>
#   View()

# Joining sus sentence minutes w/ cases
final <- df_case |>
  left_join(min_summary, by = c("id" = "case_id")) |>
  mutate(
    file_year = floor_date(date_filed, "years"),
    sus_detected = if_else(!is.na(n_sus_mins), TRUE, FALSE)
  )

# 84 cases filtered out
#check for cases being filtered out with minute regex
# For some reason it filters these out (these rows contain suspended sentence in the string) and when I checked for spelling/spacing issues I was unable to resolve the problem.
test <- df_min |>
  anti_join(
    df_case,
    by = c("case_id" = "id")) |>
  #distinct(id) |>
  View()

# annual_suspended <- join_case_min |>
#   distinct(id, year) |>
#   group_by(year) |>
#   count()
annual_suspended <- final |>
  filter(sus_detected) |>
  group_by(file_year) |>
  count()

# Check if we should be splitting OSCN counties from the rest
oscn_county_list <- c(
  "TULSA",
  "OKLAHOMA",
  "CLEVELAND",
  "ROGERS",
  "PAYNE",
  "COMANCHE",
  "GARFIELD",
  "CANADIAN",
  "LOGAN",
  "ADAIR",
  "PUSHMATAHA",
  "ROGER MILLS",
  "ELLIS"
)

oscn_cases <- df_case |>
  filter(district %in% oscn_county_list)

# check that we only have 13 OSCN counties
# join_case_min |>
#   distinct(district) |>
#   count()


# Andrew review ================================================================

# Looks like about 14ish% of all the cases in the timespan had a minute indicating a sus sentence
perc_with_sus <- nrow(final |> filter(sus_detected)) / nrow(final)

# Focused timespan:
final_focused <- final |> filter(file_year >= ymd("2014-01-01"),
                                 file_year < ymd("2019-01-01"))

perc_with_sus <- nrow(final_focused |> filter(sus_detected)) / nrow(final_focused)

# Checking results by count: ---------------------------------------------------
final |>
  count(district, sort = T)

# ...there at least some in every county it looks like, that's a good sign

# Checking results by year / county: : -----------------------------------------
final |>
  filter(sus_detected) |>
  count(file_year) |>
  ggplot(aes(x = file_year, y = n)) +
  geom_line(linewidth = 1.5) +
  scale_y_continuous(limits = c(0, NA))

final |>
  mutate(
    district = case_when(
      district %in% c("OKLAHOMA", "TULSA", "CLEVLEAND", "COMANCHE", "MUSKOGEE", "ROGERS") ~ district,
      TRUE ~ "OTHER"
    )
  ) |>
  filter(sus_detected) |>
  count(file_year, district) |>
  ggplot(aes(x = file_year, y = n, color = district)) +
  geom_line(linewidth = 1.5) +
  scale_y_continuous(limits = c(0, NA)) +
  labs(x = "Original filing year of case",
       y = "N",
       title = "Cases w/ at least one minute indicating a suspended sentence",
       subtitle = paste0(start_date, " through ", end_date))

# ...definitely getting the "older cases have had longer to accrue minutes" effect here. Sharp drop after ~2017ish
# Don't think it's a huge issue, because case filings also declined over this time and we're
# already dealing with a rough estimate. We'll just have to explain.

final |>
  filter(sus_detected) |>
  count(file_year) |>
  print(n = 22)

# Since they want "upper estimate of the number of sus sentences per year",
# I'm gonna give them an average from 2014-18. Don't want to start looking at
# 2020 and onward because those haven't had at least a few years to rack up minutes.

# Start with all cases filed...
export <- final |>
  # ...only look from 2014-2018...
  filter(file_year >= ymd("2014-01-01"),
         file_year < ymd("2019-01-01")) |>
  group_by(district) |>
  # ...summarize per district...
  summarize(
    n_total_cases = n(), # Total case filings
    n_cases_with_sus = sum(sus_detected), # Total cases w/ at least one suspended sentence related minute
    p_with_sus = n_cases_with_sus / n_total_cases, # % w/ at least one sus related minute
    n_sus_per_year = n_cases_with_sus / n_distinct(file_year), # Annual total number of sus cases
    years = paste0(min(file_year), " - ", max(file_year)), # for debugging
    n_years = n_distinct(file_year)
  )

# So in summary, we're reporting: ----------------------------------------------
# The avg. number of case filings per year,
# statewide,
# from 2014-2018,
# which went on to have at least one minute indicating a suspended sentence.

export |>
  # This just sums up the numbers over all 79 districts
  reframe(
    avg_with_sus_per_year = sum(n_sus_per_year)
  )
# or...
sum(export$n_sus_per_year)
