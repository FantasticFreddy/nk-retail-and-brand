
library(expss)
library(openxlsx)
library(labelled)
library(haven)
library(tidyverse)
library(OdysseyAnalytics)


# Department & Stores premium categorisation -----------------------------------------------------

#read categorisation

raw_survey <- read_sav("data/raw_data/NK_completes_merged_2018_06_24.sav") %>% 
 # setNames(tolower(names(.))) %>% 
  mutate(caseid = paste0("m",respondent_serial))


categorization<-read.xlsx("data/raw_data/premium_categorisation.xlsx") %>% 
#  setNames(tolower(names(.))) %>% 
  filter(premium_category != "NA") 
 # mutate(q=tolower(q))


dep <- raw_survey %>% 
select(caseid, starts_with("Q230")) %>% 
  gather(key = "q", value = value, -caseid) %>% 
  left_join(., categorization) %>% 
  filter(brand != "NA") %>% 
  select(-brand, -q, -number) %>% 
  group_by(caseid, premium_category) %>% 
  summarize(value=sum(value)) %>% 
  spread(key = premium_category, value=value) %>% 
  mutate(premium_category=case_when(
    Luxury >= 1  ~ 3,
    Affordable >= 1 ~ 2,
    Premium >= 1 ~ 1, 
    TRUE ~ 99)
  )

raw_survey_w_premium_cat <- raw_survey %>% 
  left_join(., dep, by="caseid")

# write_sav(raw_survey_w_premium_cat, "data/raw_data/raw_survey_w_premium_cat.sav")

# Read data ---------------------------------------------------------------
#rm(list=ls())
raw_survey <- read_sav("data/raw_data/raw_survey_w_premium_cat.sav") %>% 
  setNames(tolower(names(.))) %>% 
  mutate(caseid = paste0("m",respondent_serial))


sm<-raw_survey %>% 
    select(caseid, segments=segment_predicted_nk,sample=urval,varuhus=län_kodad,gender=s2,age=s1,
         weight=weight_ny,premium_category,starts_with("q400"), starts_with("q410_"), starts_with("q420_"),starts_with("q500_1_"), starts_with("Q510_1_"), starts_with("Q520_1_"),
         -q400_98, -q400_99, -'q400@_98') %>% 
  mutate(country = 1) 

names(sm)
table(sm$premium_category)

dm_survey <- openxlsx::read.xlsx("data/raw_data/datamap_retail_and_brand.xlsx") %>% 
  mutate_at(vars((-value)), str_trim, "both")


s <- sm %>% 
  zap_formats() %>% 
  zap_label() %>% 
  zap_labels() %>% 
  zap_widths()
  

demo <- sm %>% 
  select(caseid, age, gender, segments, sample, varuhus, sample, weight, country, premium_category)

# Extract OE
oe <- s %>% 
  select_if(is_character)


# Drop OE
s <- s %>%
  select(-all_of(names(select(oe, -caseid))))
 

# Tableau prep ------------------------------------------------------------

ts <- s %>% 
  #select(-all_of(drop_vars2)) %>% 
  # Transpose data
  gather(question_id2, value=value, -names(demo), na.rm = TRUE) %>% 
  # make filter var to string
  numeric_to_labels(dm_survey, select(demo, -c(caseid, age, weight)) %>% names(), "val_lab") %>% 
  # add meta data
  left_join(., dplyr::select(dm_survey, -value, -val_lab) %>% distinct(.), by = c("question_id2")) %>% 
  left_join(., dplyr::select(dm_survey, question_id2, val_lab, value), by = c("question_id2", "value")) %>% 
  # set column order
  select(caseid, question_block, question_group, question_id, 
         question_id2, var_lab, val_lab, value, age, weight,segments,country, everything())

#write.csv(ts, "data/tableau_data/nk_retail_and_brand.csv", row.names = F, na = "")

# GCS upload --------------------------------------------------------------

library(googleCloudStorageR)
library(bigrquery)

# Params
key <- "qogai-virtual-services-key.json"
gcs_auth(key)
bq_auth(path = key)

bucket <- "qogai-internal.qog.ai"
gcs_global_bucket(bucket)
project <- "qogai-virtual-services"
bq_dataset <- "nk"

# local file names 
local_survey <- "data/tableau_data/nk_retail_and_brand.csv"
# write files
write_csv(ts, local_survey, na = "", col_names = TRUE)
# GCS names
gcs_survey <- "nk/retail_and_brands/retail_and_brand.csv"
bq_survey <- "retail_and_brand"

# upload SURVEY data to Google Cloud Storage
suppressWarnings(gcs_upload(file = local_survey, bucket = bucket, name = gcs_survey))

# BQ ----------------------------------------------------------------------
# str(ts)

# Specify format for each field SURVEY
fields_segmentation <- list(bq_field(name = "caseid", type = "string"),
                            bq_field(name = "question_block", type = "string"),
                            bq_field(name = "question_group", type = "string"),
                            bq_field(name = "question_id", type = "string"),
                            bq_field(name = "question_id2", type = "string"),
                            bq_field(name = "var_lab", type = "string"),
                            bq_field(name = "val_lab", type = "string"),
                            bq_field(name = "value", type = "float"),
                            bq_field(name = "age", type = "integer"),
                            bq_field(name = "weight", type = "float"),
                            bq_field(name = "segments", type = "string"),
                            bq_field(name = "country", type = "string"),
                            bq_field(name = "gender", type = "string"),
                            bq_field(name = "premium_category", type = "string"),
                            bq_field(name = "sample", type = "string"),
                            bq_field(name = "varuhus", type = "string")
                            
)

# Push SURVEY data to BigQuery
bq_perform_load(x = bq_table(project, bq_dataset, table = bq_survey),
                fields = fields_segmentation,
                source_uris = paste0("gs://", bucket, "/", gcs_survey), 
                billing = project, 
                source_format = "CSV", 
                nskip = 1, 
                write_disposition = "WRITE_TRUNCATE")

if(file.exists(local_survey)){file.remove(local_survey)}

