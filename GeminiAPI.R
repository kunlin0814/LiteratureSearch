#install.packages("gemini.R") # CRAN
# install.packages("pak")
#pak::pak("jhk0530/gemini.R") # GitHub
library(gemini.R)
api_key <- Sys.getenv("GEMINI_API_KEY")
setAPI(api_key)
results <- gemini_chat('What is CRAN?',model = '2.5-flash',
            temperature = 1)

complete_text <- results[[2]][[2]]$parts[[1]]$text

