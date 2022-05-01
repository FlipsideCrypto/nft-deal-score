# setwd('~/git/nft-deal-score/viz')
# source("~/data_science/util/util_functions.R")
library(data.table)
library(shiny)
library(ggplot2)
library(tools)
library(plotly)
library(shinyBS)
library(tippy)
library(shinyjs)
require(dplyr)
library(htmlwidgets)
library(reactable)

plotly.style <- list(
	plot_bgcolor = "rgba(0, 0, 0, 0)", 
	paper_bgcolor = "rgba(0, 0, 0, 0)"
)
