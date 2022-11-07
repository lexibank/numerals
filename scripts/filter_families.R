#load libraries----
library(tidyverse)


#set working directory----
#setwd(getSrcDirectory()[1]) # run this line if using R
setwd(dirname(rstudioapi::getActiveDocumentContext()$path)) #run this line if using RStudio
setwd('..')


#load data----
forms <- read.csv("cldf/forms.csv")
languages <- read.csv("cldf/languages.csv", na.strings=c("","NA"))
parameters <- read.csv("cldf/parameters.csv")

colnames(languages)[12] <- "Base.Chan"
colnames(languages)[13] <- "Comment.Chan"

languages <- add_column(languages, Base.Mamta = "", .after = 12)
languages <- add_column(languages, Comment.Mamta = "", .after = 14)


#subsets for different families

#Mamta

#languages lists

languages %>%
  filter(Family == "Dravidian") -> dravidian.languages

languages %>%
  filter(Family == "Sino-Tibetan") -> sinotibetan.languages


languages %>%
  filter(Family == "Austroasiatic") -> austroasiatic.languages


#forms

forms %>%
  filter(Language_ID %in% dravidian.languages$ID) -> dravidian.forms


forms %>%
  filter(Language_ID %in% sinotibetan.languages$ID) -> sinotibetan.forms


forms %>%
  filter(Language_ID %in% austroasiatic.languages$ID) -> austroasiatic.forms

if (!dir.exists("families")) { dir.create("families") }


write.csv(dravidian.languages, "families/dravidian.languages.csv")
write.csv(sinotibetan.languages, "families/sinotibetan.languages.csv")
write.csv(austroasiatic.languages, "families/austroasiatic.languages.csv")


write.csv(dravidian.forms, "families/dravidian.forms.csv",fileEncoding = "UTF-8")
write.csv(sinotibetan.forms, "families/sinotibetan.forms.csv",fileEncoding = "UTF-8")
write.csv(austroasiatic.forms, "families/austroasiatic.forms.csv",fileEncoding = "UTF-8")


#Enock

colnames(languages)[c(13,15)] <- c("Base.Enock", "Comment.Enock")

languages %>%
  filter(Family == "Atlantic-Congo") -> atlanticcongo.languages

write.csv(atlanticcongo.languages, "families/atlanticcongo.languages.csv")


