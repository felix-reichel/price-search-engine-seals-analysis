library(ggplot2)
library(dplyr)
library(plotly)
library(rstudioapi)

setwd(dirname(getActiveDocumentContext()$path))

data <- read.csv("results.csv")

data <- data %>%
  mutate(
    produkt_id = as.numeric(factor(produkt_id)) - 1,
    haendler_bez = as.numeric(factor(haendler_bez)) - 1,
    week_running_var = as.numeric(factor(week_running_var)) - 1
  )

cube_data <- data %>%
  group_by(produkt_id, haendler_bez, week_running_var) %>%
  summarise(count = n()) %>%
  ungroup()

slice_by_product_retailer <- function(data, time_period) {
  slice <- data %>%
    filter(week_running_var == time_period)
  
  plot <- ggplot(slice, aes(x = haendler_bez, y = produkt_id, fill = count)) +
    geom_tile(color = "white") +
    scale_fill_gradient(low = "white", high = "red") +
    labs(x = "Retailer", y = "Product") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 90, hjust = 1))
  
  print(plot)
  ggsave(paste0("slice_product_retailer_time_", time_period, ".png"))
}

slice_by_retailer_time <- function(data, product) {
  slice <- data %>%
    filter(produkt_id == product)
  
  plot <- ggplot(slice, aes(x = week_running_var, y = haendler_bez, fill = count)) +
    geom_tile(color = "white") +
    scale_fill_gradient(low = "white", high = "red") +
    labs(x = "Time Period", y = "Retailer") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 90, hjust = 1))
  
  print(plot)
  ggsave(paste0("slice_retailer_time_product_", product, ".png"))
}

cube_visualization <- function(data) {
  plot <- plot_ly(
    data = data,
    x = ~haendler_bez,
    y = ~produkt_id,
    z = ~week_running_var,
    marker = list(size = 3),
    color = ~count,
    colors = colorRamp(c("white", "red")),
    type = "scatter3d",
    mode = "markers"
  ) %>%
    layout(
      scene = list(
        xaxis = list(title = "Retailer (j)"),
        yaxis = list(title = "Product (i)"),
        zaxis = list(title = "Time (t)")
      ),
      title = "3D Cube of Retailer (j), Product (i), and Time (t)"
    )
  
  plot
  htmlwidgets::saveWidget(plot, "cube_visualization.html")
}

slice_and_dice_visualizer <- function(data) {
  slice_by_product_retailer(data, 10)
  slice_by_retailer_time(data, 0)
  cube_visualization(data)
}

slice_and_dice_visualizer(cube_data)
