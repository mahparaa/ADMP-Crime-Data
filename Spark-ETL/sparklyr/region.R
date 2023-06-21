library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)




regions_df <- dbReadTable(con, "region")
regions_df <- sdf_copy_to(sc, leaver_df, "spark_region", overwrite = TRUE)

sc <- spark_connect(master = "local")

region_path <- "C:\\Users\\LabStudent-55-706949\\Desktop\\CrimeDataset\\regions.csv"
region_df = spark_read_csv(sc, "spark_region", region_path)

regions_df <- regions_df %>%
  select(-region.id, -region.id2) %>%
  distinct_all()

regions_df %>% count()

spark_disconnect(sc)

