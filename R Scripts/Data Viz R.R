library(xlsx)
library(tidyverse)
library(plyr)
library(RMySQL)

install.packages("RMySQL")
install.packages("ggplot2")

db_user <- 'tickets_user'
db_pass <- 'tickets_pass'
db_name <- 'tickets_db'
db_host <- 'ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com'
db_port <- 3306

tickets_db <- dbConnect(MySQL(), 
                        user = db_user,
                        password = db_pass,
                        dbname = db_name,
                        host = db_host,
                        port = db_port)

query_string <- paste0("select * from STUBHUB_EVENTBRITE_join")
query_submit <- dbSendQuery(tickets_db, query_string)
diplo_df <- fetch(query_submit, n=-1)
on.exit(dbDisconnect(tickets_db))

print(df)

good_example <- 