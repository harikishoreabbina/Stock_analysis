# Databricks notebook source
# MAGIC %md
# MAGIC # Extrating the data 

# COMMAND ----------

import yfinance as yf
import pandas as pd

# Function to fetch historical data for a ticker
def fetch_historical_data(ticker, start_date="2000-01-01", end_date="2024-12-15"):
    return yf.download(ticker, start=start_date, end=end_date)

# Function to fetch stock metadata
def fetch_metadata(ticker):
    stock = yf.Ticker(ticker)
    info = stock.info
    industry = info.get("industry", "N/A")
    outstanding_shares = info.get("sharesOutstanding", 1)
    return industry, outstanding_shares

# Function to process data: Add calculated fields and rolling metrics
def process_data(data, industry, outstanding_shares):
    data["Industry"] = industry
    data["Market Cap"] = data["Close"] * outstanding_shares
    data["52-Week High"] = data["Close"].rolling(window=252, min_periods=1).max()
    data["52-Week Low"] = data["Close"].rolling(window=252, min_periods=1).min()
    data["SMA_50"] = data["Close"].rolling(window=50, min_periods=1).mean()
    data["SMA_200"] = data["Close"].rolling(window=200, min_periods=1).mean()
    return data

# Function to save data to a CSV file
def save_to_csv(data, ticker):
    data.reset_index(inplace=True)
    data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
    file_name = f"{ticker.replace('.BO', '')}_data.csv"
    required_columns = [
        "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume",
        "Industry", "Market Cap", "52-Week High", "52-Week Low",
        "SMA_50", "SMA_200"
    ]
    data[required_columns].to_csv(file_name, index=False)
    print(f"Saved data for {ticker} to {file_name}")

# Main function to handle multiple tickers
def fetch_and_save_all_tickers(tickers):
    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            # Fetch historical data
            historical_data = fetch_historical_data(ticker)
            
            # Fetch metadata
            industry, outstanding_shares = fetch_metadata(ticker)
            
            # Process data
            processed_data = process_data(historical_data, industry, outstanding_shares)
            
            # Save to CSV
            save_to_csv(processed_data, ticker)
        
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

# List of tickers
tickers = [
    "RELIANCE.BO", "TCS.BO", "INFY.BO", "HDFCBANK.BO", "ICICIBANK.BO",
    "SBIN.BO", "AXISBANK.BO", "ITC.BO", "HINDUNILVR.BO", "BAJFINANCE.BO",
    "KOTAKBANK.BO", "ADANIENT.BO", "LT.BO", "WIPRO.BO", "SUNPHARMA.BO",
    "MARUTI.BO", "BHARTIARTL.BO", "POWERGRID.BO", "NTPC.BO", "ONGC.BO"
]

# Run the process for all tickers
fetch_and_save_all_tickers(tickers)


# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC ### ## Import the data into your database. Show the number of Rows and Columns using Database Queries or Python.

# COMMAND ----------

import pandas as pd
import os


data_dir = "./" 


combined_data = []


for ticker in tickers:
    file_name = f"{ticker.replace('.BO', '')}_data.csv"  # File name for the company data
    file_path = os.path.join(data_dir, file_name)  # Full path to the file
    
    if os.path.exists(file_path):  # Check if file exists
        print(f"Processing file: {file_name}")
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Add a new column "Company" with the company name
        company_name = ticker.replace(".BO", "")  # Extract clean company name
        df["Company"] = company_name
        
        # Append to the combined_data list
        combined_data.append(df)
    else:
        print(f"File not found: {file_name}")

# Combine all the data into a single DataFrame
if combined_data:
    final_data = pd.concat(combined_data, ignore_index=True)
    
    # Save the combined data to a new CSV
    final_file = "combined_top20_companies_data.csv"
    final_data.to_csv(final_file, index=False)
    print(f"Combined data saved to: {final_file}")
else:
    print("No data files were processed.")


# COMMAND ----------

 final_data.shape

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, DecimalType, StringType
from decimal import Decimal
import pandas as pd

# Define schema with higher precision
schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DecimalType(38, 2), True),
    StructField("High", DecimalType(38, 2), True),
    StructField("Low", DecimalType(38, 2), True),
    StructField("Close", DecimalType(38, 2), True),
    StructField("Adj Close", DecimalType(38, 2), True),
    StructField("Volume", DecimalType(38, 0), True),
    StructField("Industry", StringType(), True),
    StructField("Market Cap", DecimalType(38, 0), True),
    StructField("52-Week High", DecimalType(38, 2), True),
    StructField("52-Week Low", DecimalType(38, 2), True),
    StructField("SMA_50", DecimalType(38, 2), True),
    StructField("SMA_200", DecimalType(38, 2), True),
    StructField("Company", StringType(), True)
])

# Convert float columns to Decimal
for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Market Cap", "52-Week High", "52-Week Low", "SMA_50", "SMA_200"]:
    final_data[col] = final_data[col].apply(lambda x: Decimal(x) if pd.notnull(x) else None)

# Load the data
final_data['Date'] = pd.to_datetime(final_data['Date'])
df = spark.createDataFrame(final_data, schema=schema)

# Verify Schema
df.printSchema()

# Show data
display(df.limit(5))

# COMMAND ----------

rows_count = df.count()
columns_count = len(df.columns)

print(f"The DataFrame has {rows_count} rows and {columns_count} columns.")


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer
# MAGIC ### Clean the data 

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType, FloatType

# Count null values for each column
null_counts = df.select(
    [count(when(col(c).isNull() | (isnan(col(c)) if df.schema[c].dataType in [DoubleType(), FloatType()] else False), c)).alias(c) for c in df.columns]
)

# Display null counts
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType, FloatType

# Count null values for each column
null_counts = df.select(
    [count(when(col(c).isNotNull() | (isnan(col(c)) if df.schema[c].dataType in [DoubleType(), FloatType()] else False), c)).alias(c) for c in df.columns]
)

# Display null counts
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter numeric columns
numeric_columns = [c[0] for c in df.dtypes if c[1].startswith('decimal')]

# Select only numeric columns
numeric_df = df.select(*[col(c) for c in numeric_columns])

# Calculate min and max for each numeric column
summary_df = numeric_df.summary("min", "max")


display(summary_df)


# COMMAND ----------

from pyspark.sql.functions import col

# Filter rows where 'Adj Close' is less than 0
negative_adj_close_count = df.filter(col("Adj Close") < 0).count()

# Print the result
print(f"Number of rows with 'Adj Close' less than 0: {negative_adj_close_count}")


# COMMAND ----------

from pyspark.sql.functions import col

# Filter rows where 'Adj Close' equals 0
zero_adj_close_rows = df.filter(col("Adj Close") <0)

# Show the rows
display(zero_adj_close_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### So filling with 0 will be best case where the Adj<0
# MAGIC The adjusted price reflects the stock's closing price after accounting for corporate actions like dividends, stock splits, and similar events. These adjustments are proportional and cannot yield a negative price since stock prices themselves are non-negative.
# MAGIC

# COMMAND ----------


df = df.withColumn("Adj Close", when(col("Adj Close") < 0, 0).otherwise(col("Adj Close")))

# Verify there are no negative values in 'Adj Close'
negative_count = df.filter(col("Adj Close") < 0).count()
print(f"Number of rows with 'Adj Close' less than 0 after replacement: {negative_count}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Daily Change and Daily % Change

# COMMAND ----------



df = (
    df.withColumn("Daily Change", col("Close") - col("Open"))
      .withColumn("Daily % Change", ((col("Close") - col("Open")) / col("Open")) * 100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Bollinger Bands:

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev, lit, when

window_size = 10
k = 1.5

rolling_window = Window.orderBy("Date").rowsBetween(-(window_size-1), 0)

df = df.withColumn("SMA", avg(col("Close")).over(rolling_window))
df = df.withColumn("StdDev", stddev(col("Close")).over(rolling_window))
df = df.withColumn("Upper Band", col("SMA") + (lit(k) * col("StdDev")))
df = df.withColumn("Lower Band", col("SMA") - (lit(k) * col("StdDev")))

df = df.withColumn("StdDev", when(col("StdDev").isNull(), 0).otherwise(col("StdDev")))
df = df.withColumn("Upper Band", when(col("Upper Band").isNull(), 0).otherwise(col("Upper Band")))
df = df.withColumn("Lower Band", when(col("Lower Band").isNull(), 0).otherwise(col("Lower Band")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some other Parameter 

# COMMAND ----------

#Daily Price Range:
df = df.withColumn("Daily Range", col("High") - col("Low"))
#Volatility Percentage:
df = df.withColumn("Volatility %", ((col("High") - col("Low")) / col("Open")) * 100)



# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType, FloatType


null_counts = df.select(
    [count(when(col(c).isNull() | (isnan(col(c)) if df.schema[c].dataType in [DoubleType(), FloatType()] else False), c)).alias(c) for c in df.columns]
)


row_count = df.count()


summary_counts = spark.createDataFrame(
    [(row_count, len(df.columns))],
    ["Row Count", "Column Count"]
)


display(null_counts)
display(summary_counts)
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why its important to invest?

# COMMAND ----------

from pyspark.sql.functions import col, first, last, lit, round, sum as spark_sum
import matplotlib.pyplot as plt

# Initial investment amount
initial_investment = 1

# Group by company and aggregate data
aggregated_df = df.groupBy("Company").agg(
    round(first("Open"), 2).alias("Starting Price"),
    round(last("Close"), 2).alias("Final Price"),
    round((((last("Close") - first("Open")) / first("Open")) * 100), 2).alias("Percent Growth"),
)

# Add the Present Value column with rounding
aggregated_df = aggregated_df.withColumn(
    "Present Value",
    round(lit(initial_investment) * (1 + col("Percent Growth") / 100), 2)
)

# Calculate the total sum of the present values
total_present_value = aggregated_df.agg(spark_sum("Present Value").alias("Total Present Value")).collect()[0]["Total Present Value"]
print(f"Total sum of the present values: {total_present_value}")

# Convert to Pandas DataFrame for plotting
pandas_df = aggregated_df.select("Company", "Present Value").toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(pandas_df["Company"], pandas_df["Present Value"])
plt.xlabel("Company")
plt.ylabel("Present Value")
plt.title("Present Value by Company")
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# Import the built-in round function explicitly
from builtins import round

# Parameters
initial_investment = 20
growth_rate = 0.10  # 10%
years = 24

# Calculate future value
future_value = initial_investment * ((1 + growth_rate) ** years)
print(f"The future value after {years} years is: {round(future_value, 2)}")


# COMMAND ----------

from pyspark.sql.functions import col, first, last, lit, round
import matplotlib.pyplot as plt

# Initial investment amount
initial_investment = 1

# Group by Company and Industry, and aggregate data
aggregated_df = df.groupBy("Company", "Industry").agg(
    round(first("Open"), 2).alias("Starting Price"),
    round(last("Close"), 2).alias("Final Price"),
    round((((last("Close") - first("Open")) / first("Open")) * 100), 2).alias("Percent Growth")
)

# Add the Present Value column with rounding
aggregated_df = aggregated_df.withColumn(
    "Present Value",
    round(lit(initial_investment) * (1 + col("Percent Growth") / 100), 2)
)

# Convert to Pandas DataFrame for plotting
pandas_df = aggregated_df.select("Company", "Present Value").toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.pie(pandas_df["Present Value"], labels=pandas_df["Company"], autopct='%1.1f%%', startangle=140)
plt.title("Present Value by Company")
plt.axis('equal')
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col, row_number, round
from pyspark.sql.window import Window

# Define the window specification
window_spec = Window.partitionBy("Company").orderBy(col("Daily % Change").desc())

# Rank rows and select the highest Daily % Change
df_ranked = df.withColumn("Rank", row_number().over(window_spec))

# Filter for Rank 1 and round the Daily % Change to 2 decimals
result_df = df_ranked.filter(col("Rank") == 1).select(
    "Company",
    "Date",
    "Industry",
    round(col("Daily % Change"), 2).alias("Daily % Change")  # Round to 2 decimal places
)

# Show the result
display(result_df.limit(5))


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Volatility and Daily change

# COMMAND ----------

from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt

# Step 1: Group data by Date and calculate averages
aggregated_df = df.groupBy("Date").agg(
    avg(col("Volatility %")).alias("Average Volatility"),
    avg(col("Daily % Change")).alias("Average Daily Change")
)

# Step 2: Convert the aggregated Spark DataFrame to Pandas for plotting
pandas_df = aggregated_df.orderBy("Date").toPandas()

# Step 3: Scatter Plot
plt.figure(figsize=(12, 6))

# Scatter for Average Volatility
plt.scatter(pandas_df["Date"], pandas_df["Average Volatility"], label="Average Volatility", color='tab:blue', alpha=0.7)

# Scatter for Average Daily Change
plt.scatter(pandas_df["Date"], pandas_df["Average Daily Change"], label="Average Daily Change", color='tab:orange', alpha=0.7)

# Customizations
plt.title("Scatter Plot: Average Volatility vs Average Daily Change")
plt.xlabel("Date")
plt.ylabel("Percentage (%)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql.functions import col, avg, year
import matplotlib.pyplot as plt

# Step 1: Filter for year 2024 and group data by Date
aggregated_df = df.filter(year(col("Date")) == 2024).groupBy("Date").agg(
    avg(col("Volatility %")).alias("Average Volatility"),
    avg(col("Daily % Change")).alias("Average Daily Change")
)

# Step 2: Convert the aggregated Spark DataFrame to Pandas for plotting
pandas_df = aggregated_df.orderBy("Date").toPandas()

# Step 3: Plot the scatter plot using Matplotlib
plt.figure(figsize=(12, 6))

# Scatter plot for Average Volatility
plt.scatter(pandas_df["Date"], pandas_df["Average Volatility"], 
            label="Average Volatility", color="tab:blue", alpha=0.7)

# Scatter plot for Average Daily Change
plt.scatter(pandas_df["Date"], pandas_df["Average Daily Change"], 
            label="Average Daily Change", color="tab:orange", alpha=0.7)

# Customizations
plt.title("Scatter Plot: Average Volatility vs Average Daily Change (2024)")
plt.xlabel("Date")
plt.ylabel("Percentage (%)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.xticks(rotation=45)  # Rotate x-axis dates for better readability
plt.tight_layout()

# Show the plot
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bollinger Bands

# COMMAND ----------

import matplotlib.pyplot as plt

# Get a distinct list of companies from the DataFrame
companies = [row["Company"] for row in df.select("Company").distinct().collect()]

# Loop through each company and plot Bollinger Bands
for company in companies:
    # Filter the data for the current company
    company_df = df.filter(col("Company") == company).select("Date", "Close", "SMA", "Upper Band", "Lower Band").orderBy("Date").toPandas()
    
    # Ensure 'Date' is treated as datetime
    company_df['Date'] = company_df['Date'].astype('datetime64[ns]')
    
    # Plotting Bollinger Bands
    plt.figure(figsize=(12, 6))
    plt.plot(company_df['Date'], company_df['Close'], label='Close Price', linewidth=1.5)
    plt.plot(company_df['Date'], company_df['SMA'], label='SMA', linestyle='--', linewidth=1.5)
    plt.plot(company_daf['Date'], company_df['Upper Band'], label='Upper Band', color='red', linewidth=1)
    plt.plot(company_df['Date'], company_df['Lower Band'], label='Lower Band', color='green', linewidth=1)
    plt.fill_between(company_df['Date'], company_df['Upper Band'], company_df['Lower Band'], color='gray', alpha=0.2)
    
    # Graph customization
    plt.title(f'Bollinger Bands for {company}')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid()
    plt.tight_layout()
    
    # Show the plot for each company
    plt.show()

