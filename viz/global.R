# TODO: look at stability over time
#setwd("~/git/nft-deal-score/viz")
#source("~/data_science/util/util_functions.R")
# library(DT)
library(data.table)
library(shiny)
library(ggplot2)
# library(scales)
# library(shinyWidgets)
library(plotly)
# library(bslib)
# require(reshape2)
require(dplyr)
# require(RPostgreSQL)
# library(RJSONIO)
# library(stringr)
# library(showtext)
# library(fmsb)
library(reactable)
# font_add_google(name = "Roboto Condensed", family = "roboto-condensed")
# font_add_google(name = "Roboto Mono", family = "roboto-mono")
# showtext_auto()

# library(elementalist)

BG_COLOR = '#282923'

plotly.style <- list(
  fig_bgcolor = "rgb(255, 255, 255)", 
  plot_bgcolor = "rgba(0, 0, 0, 0)", 
  paper_bgcolor = "rgba(0, 0, 0, 0)",
  font = list(
    color = '#919EAB', family = "Roboto Mono")
)


getTokenAddress <- function(x) {
	token_address <- tryCatch(
		{
			strsplit( as.character(x), "\\(|\\)")[[1]][2]
		},
		error = function(e){ 
		},
		warning = function(w){
		},
		finally = {
		}
		
	)
	return(token_address)
}

getTokenSymbol <- function(x) {
	token_address <- tryCatch(
		{
			strsplit( as.character(x), "\\(|\\)")[[1]][1]
		},
		error = function(e){ 
		},
		warning = function(w){
		},
		finally = {
		}
		
	)
	return(token_address)
}

printNumber <- function(x) {
	return(prettyNum(x, format='d', big.mark=',', digits=5))
}

printPercent <- function(x) {
	return(label_percent()(x))
}

