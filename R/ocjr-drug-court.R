

#___________________________Drug Court_________________________________ 
# Trying to find evidence on SQ780's effect on drug court participation
drug_court <- ojo_tbl("minute") |> 
  filter(str_detect(description, "(?i)drug court")) |> 
  ojo_collect()

# Search through the minute data:
# How many CF / CM cases had a DCPF drug court program fee minute in Oklahoma county pre- and post-780? 
dcpf <- ojo_tbl("minute") |> 
  filter(str_detect(description, "(?i)DCPF")) |> 
  ojo_collect()
# What is that as a % of total CF / CM cases?

# How many CF / CM cases had a minute indicating that they were removed from drug court? E.g. "MOTION TO REVOKE FROM DRUG COURT", "BENCH WARRANT - DRUG COURT VIOLATION", etc. Get total numbers and numbers as a % of all cases pre- and post-780.

