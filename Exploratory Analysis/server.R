#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

library(shiny)
library(ggplot2)

# Define server logic required to draw a histogram
shinyServer(function(input, output) {
  data <- reactive({
    get(input$data_name)
  })
  
  output$data <- renderDataTable({
    data()
  })
  output$choose_x_component <- renderUI({
    selectInput("x_component","Choose X component",names(data()))
  })
  output$choose_y_component <- renderUI({
    selectInput("y_component","Choose Y component",names(data()),
                selected =names(data())[2])
  })
  output$choose_group <- renderUI({
    selectInput("group","Choose group",names(data()),
                selected =names(data())[2])
  })
  
  x_component <- reactive({
    input$x_component
  })
  y_component <- reactive({
    input$y_component
  })
  colour <- reactive({
    input$group
  })
  output$check <- renderText({
    paste(paste(x_component(),y_component()),colour())
  })
  
  output$structure <- renderPrint({
    str(data())
  })
  output$summary_ui <- renderUI({
    text <- paste(paste("Summary of",x_component()),"(X-Component)") 
    h3(text)
  })
  output$summary <- renderPrint({
    summary(data()[[x_component()]])
  })
  output$plot1=renderPlot({
    d <- data()
    title <- paste(paste(x_component(),"VS"),y_component())
    grouped_var <- factor(d[[colour()]])
    g <- ggplot(data=d) + aes_string(x=x_component(),y=y_component())
    g <- g + geom_point(aes(colour=grouped_var),size=2) + labs(title=title)+
      theme(plot.title = element_text(hjust = 0.5))
    if(input$linModelCheck==T){
      g <- g + geom_smooth(method = "lm",se=F) 
    }
    g
  })
  
})
