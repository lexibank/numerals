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

languages %>%
  filter(Family == "Dravidian") -> dravidian.languages

languages %>%
  filter(Family == "Sino-Tibetan") -> sinotibetan.languages


languages %>%
  filter(Family == "Austroasiatic") -> austroasiatic.languages

if (!dir.exists("families")) { dir.create("families") }


write.csv(dravidian.languages, "families/dravidian.languages.csv")
write.csv(sinotibetan.languages, "families/sinotibetan.languages.csv")
write.csv(austroasiatic.languages, "families/austroasiatic.languages.csv")

