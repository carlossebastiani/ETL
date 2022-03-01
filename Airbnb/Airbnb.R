# install.packages("dbplyr")
library(dplyr)
library(dbplyr)
library(stringr)
library(tidyverse)



# Creamos una base de datos en memoria.
conn <- DBI::dbConnect(RSQLite::SQLite(), "../Airbnb/data/airbnb.sqlite")

# En este caso, nuestra tabla en un dataframe predefinido en R.
DBI::dbListTables(conn)

#EXTRACCIÓN
# SELECCION CORRECTA PARTE 1 ----------------------------------------------


listings <- tbl(conn, sql("SELECT Listings.price,
                          Listings.number_of_reviews,
                          Listings.review_scores_rating,
                          Listings.room_type,
                          Hoods.neighbourhood_group,
                          Listings.id
                           FROM Listings 
                           INNER JOIN Hoods 
                           on Listings.neighbourhood_cleansed = Hoods.neighbourhood
                           "))
listings <- listings %>% collect()


# PARTE 2 SELECCION -------------------------------------------------------

#Hacemos queries para acceder a los datos
reviews <- as.data.frame(tbl(conn, sql("SELECT
                          Hoods.neighbourhood_group,
                          strftime('%Y-%m', date) as mes, 
                          Listings.price, 
                          COUNT(Reviews.id) number_of_reviews , 
                          Listings.review_scores_rating
                           FROM Reviews 
                            INNER JOIN Listings 
                            on Reviews.listing_id = Listings.id
                              INNER JOIN Hoods 
                              on Listings.neighbourhood_cleansed = Hoods.neighbourhood
                          WHERE mes > '2010-12'
                          GROUP BY Hoods.neighbourhood_group, mes
                           ")))

reviews <- reviews %>% collect()

# TRANSFORMACIÓN ----------------------------------------------------------

# Quitando los "$" y las ","
 
listings <- listings %>% 
  mutate(price=str_replace(price,"\\$",""),
         price=str_replace(price,",",""),
         price=as.numeric(price))
#check
head(listings)

#OPCION A 
#Cleaning NAs of number_of_reviews

number_of_reviews2 = {} # Creo una lista vacía
for (i in listings$number_of_reviews) {
  if (!is.na(i)) {
    number_of_reviews2 <- append(number_of_reviews2,i)
  } else {
    #asingo un random value con la restricción de que no sea NA
    s <- sample(listings$number_of_reviews[!is.na(listings$number_of_reviews)],1) 
    # Lo incluyo en la lista
    number_of_reviews2 <- append(number_of_reviews2,s)
  }
}

head(number_of_reviews2)

#Cleaning NAs of review_score_rating

review_scores_rating2 = {} # Creo una lista vacía
for (i in listings$review_scores_rating) {
  if (!is.na(i)) {
    review_scores_rating2 <- append(review_scores_rating2,i)
  } else {
    #asingo un random value con la restricción de que no sea NA
    s <- sample(listings$review_scores_rating[!is.na(listings$review_scores_rating)],1) 
    # Lo incluyo en la lista
    review_scores_rating2 <- append(review_scores_rating2,s)
  }
}

head(review_scores_rating2)
#Cambiamos el data frame

listings <- listings %>% 
  mutate(number_of_reviews=number_of_reviews2,
         review_scores_rating = review_scores_rating2)
#Comprobamos
head(listings)


# Tranformación Listings 2 ------------------------------------------------
listings %>% 
  group_by(neighbourhood_group, room_type) %>%
  summarise(avg_rating_score=weighted.mean(review_scores_rating,number_of_reviews),
            median_price=median(price)) 


# Transformation Reviews --------------------------------------------------

# Hacemos un nuevo DF
august <- reviews %>% 
  filter(mes=="2021-07")

#Hacemos el nuevo array para luego hacer el reeemplazo
month_august <- {}
for (i in 1:nrow(august)){
  month_august <- append(month_august,"2021-08")
}

#ponemos el nombre a la columna
head(month_august)
august <- august %>% 
  mutate(mes=month_august)

# Unimos los Data Frames
reviews <- bind_rows(reviews,august)

#Ordenamos por neighbour y mes(el mes ya está ordenado)
reviews<- reviews %>% 
  arrange(neighbourhood_group)
 
## ULTIMA TRANSFORMACIÓN
#Creamos un listado de fechas posibles con el formato correcto, 
#es decir, con YYYY-MM y que esté en formato texto
fechas <- char(format(seq(as.Date("2011-01-01"), as.Date("2021-07-01"), by="months"), "%Y-%m"))

#Sacamos los distritos unicos
unique_neighbourhoods<- unique(reviews$neighbourhood_group)

#Unimos las dos utilizando un expand_grid
uniques <- expand_grid(unique_neighbourhoods, fechas)
#Cambiamos el nombre de las columnas para que sean iguales al del dataframe
colnames(uniques) <- c("neighbourhood_group", "mes")
#Check
uniques

#Hacemos un full join para quedarnos con todos los datos
reviews = merge(reviews,uniques,by=c("mes","neighbourhood_group"),no.dups = TRUE,all=TRUE)

#Reemplazamos Nas por 0
reviews[is.na(reviews)] <- 0
head(reviews)

DBI::dbWriteTable(con = conn, name = 'listings_fixed', value= listings, overwrite=TRUE)
DBI::dbWriteTable(con = conn, name = 'reviews_fixed', value= reviews, overwrite=TRUE)

check1 <- tbl(conn, sql("SELECT * 
                             FROM listings_fixed
                             LIMIT 10"))  
check1 %>% collect() 

check2 <- tbl(conn, sql("SELECT * 
                             FROM reviews_fixed
                              LIMIT 10")) 
check2 %>% collect() 

