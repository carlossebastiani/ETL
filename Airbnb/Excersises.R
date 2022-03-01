#Ejercicio 2 

library(readr)
library(dplyr)
library(zoo)
df <- read_csv("../Ejercicios/hipotecas_lectura",skip=7,col_names =headers,n_max = 12)
headers= read.csv("../Ejercicios/hipotecas_lectura",nrows = 1,header = F)
colnames(df)=headers
df


# SQL ---------------------------------------------------------------------



library(dplyr)

# Creamos una base de datos en memoria.
conn <- DBI::dbConnect(RSQLite::SQLite(), "../Ejercicios/indexKaggle (1).sqlite")

DBI::dbListTables(conn)

IndexPrice <- tbl(conn, sql("SELECT * FROM IndexPrice LIMIT 10"))
IndexPrice <- IndexPrice %>% collect()
View(IndexPrice)

IndexMeta<- tbl(conn, sql("SELECT * FROM IndexMeta LIMIT 10"))
IndexMeta <- IndexMeta %>% collect()
View(IndexMeta)

IP_EEUU_EU <- tbl(conn, sql("SELECT p.stock_index, p.date, p.adj_close, p.volume FROM IndexPrice p
                            INNER JOIN 
                            IndexMeta m on p.stock_index =m.stock_index
                            WHERE m.region in ('United States', 'Europe')
                            AND date >= '2019-01-01'
                            "))
IP_EEUU_EU <- IP_EEUU_EU %>% collect()
View(IP_EEUU_EU)




  vol_p <- tbl(conn, sql("SELECT p.stock_index, p.date, p.adj_close, p.volume, m.currency FROM IndexMeta m 
  INNER JOIN IndexPrice p on m.stock_index = p.stock_index 
  WHERE p.date >= '2007-01-01' AND p.date <= '2011-01-01'
  AND m.currency in ('USD', 'EUR')
                            "))
vol_p


# Sparkly R ---------------------------------------------------------------





# Transform ---------------------------------------------------------------

set.seed(5678)
vector_letters <- sample(letters, 50, TRUE)
vector_letters[sample(seq_len(50), 25)] <- NA

vector_letters %>% 
  mutate(vector_letters_lag = lag(vector_letters, n = 2), # 1 period
                  vector_letters_fixed = if_else(is.na(V5), 
                                                 vector_letters_lag, 
                                                 vector_letters))

lag(vector_letters, n = 2)

na.locf(vector_letters)


#Extract Pets


df2 <- read_csv("../Ejercicios/P9-ProceduresHistory.csv")

df2 <- df2 %>% 
  count(Date) %>% 
  group_by(Date)

df3 <- df2 %>% 
  mutate(Sunday=format(Date, format = "%u"),
         Sunday= if (Sunday==7) 1 else 0)

class(df3['Sunday'])

  






