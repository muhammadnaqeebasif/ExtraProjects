#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(shiny)

# Define UI for application that draws a histogram
shinyUI(fluidPage(
  titlePanel("Exploratory Analysis"),
  sidebarLayout(
    sidebarPanel(
      selectInput("data_name","Select data",c("mtcars","iris","CO2")),
      uiOutput("choose_x_component"),
      uiOutput("choose_y_component"),
      uiOutput("choose_group"),
      checkboxInput("linModelCheck","Linear Modelling")
    ),
    mainPanel(
      h3("Structure of the data"),
      verbatimTextOutput("structure"),
      uiOutput("summary_ui"),
      verbatimTextOutput("summary"),
      h3("Plot"),
      plotOutput("plot1")
    ))
))
