import ast
import io
import os
import zipfile
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Load environment variables
load_dotenv()

zip_url = os.getenv("DATASOURCE")

# Initialize Spark Session
spark = SparkSession\
    .builder \
    .appName("Movie_Analytics") \
    .config("spark.jars", os.getenv("MYSQL_CONNECTOR_JAR")) \
    .getOrCreate()

# Database connection properties
jdbc_url = os.getenv("JDBC_URL")

properties = {
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

def extract_data(zip_url):
    """
    Extracts CSV and JSON files from a ZIP file located at the provided URL.
    Only files within the 'project_data/' directory are processed.
    Returns a tuple of three DataFrames (movies_main_df, movie_extended_df, ratings_df)
    """

    response = requests.get(zip_url)

    if response.status_code == 200:
        buffer = io.BytesIO(response.content)
        dataframes = {}

        with zipfile.ZipFile(buffer, "r") as zip_ref:
            for file_name in zip_ref.namelist():

                if file_name.startswith("project_data/") and not file_name.endswith("/"):
                    print("Processing:", file_name)
                    clean_name = file_name.split("/")[-1].split(".")[0]

                    with zip_ref.open(file_name) as f:
                        if file_name.endswith(".csv"):
                            dataframes[f"{clean_name}_df"] = pd.read_csv(f)

                        elif file_name.endswith(".json"):
                            dataframes[f"{clean_name}_df"] = pd.read_json(f)

        print("\nExtracted the data into a pandas dataframe")
        return (dataframes.get("movies_main_df"), 
                dataframes.get("movie_extended_df"), 
                dataframes.get("ratings_df"))

    else:
        print("Failed to download the ZIP file")
        return None, None, None
def transform_dim_movies(df):
    """
    Transforms the movie DataFrame into a dimension table, and returns the dim_movies dataframe
    """
    
    df = df[df["id"].str.isnumeric()]
    df.loc[:, "release_date"] = pd.to_datetime(df["release_date"], format="mixed").dt.strftime("%m/%d/%Y")
    df = df.drop(columns=["budget", "revenue"]).rename(columns={"id": "movie_id"})
    df["movie_id"] = df["movie_id"].astype(int)
    df.dropna(subset=["movie_id", "title"], inplace=True)

    return df
def transform_dim_genres(df):
    """
    Transforms the genre information into a dimension table, and returns the dim_genres dataframe.
    """
        
    df["genres"] = df["genres"].str.split(",").reset_index(drop=True)
    df = df.explode("genres")
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.drop(columns=["production_companies", "production_countries", "spoken_languages"]).rename(columns={"id": "movie_id", "genres": "genre"}).dropna()
    df["movie_id"] = df["movie_id"].astype(int)

    return df
def transform_dim_companies(df):
    """
    Transforms the production company information into a dimension table, and returns the dim_companies dataframe.
    """
    
    df["production_companies"] = df["production_companies"].str.split(",").reset_index(drop=True)
    df = df.explode("production_companies")
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.drop(columns=["genres", "production_countries", "spoken_languages"]).rename(columns={"id": "movie_id", "production_companies": "production_company"}).dropna()
    df["movie_id"] = df["movie_id"].astype(int)

    return df
def safe_eval(val):
    """
    Safely evaluates a string representation of a list or dictionary into an object,
    for the purpose of columns with stringified dictionary as a data point (For stringified lists/dictionaries datapoints).
    """

    if isinstance(val, str):
        return ast.literal_eval(val)
    
    return val
def transform_dim_countries(df):
    """
    Transforms country information into a dimension table, and returns the dim_countries dataframe.
    """
    
    df["production_countries"] = df["production_countries"].apply(safe_eval)
    df = df.explode("production_countries")
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df .dropna(subset=["production_countries"]).reset_index(drop=True)
    df = pd.concat([df["id"], pd.json_normalize(df["production_countries"])], axis=1).rename(columns={"id": "movie_id", "iso_3166_1": "country_code", "name": "country"}).dropna()
    df["movie_id"] = df["movie_id"].astype(int)
    
    return df
def transform_dim_languages(df):
    """
    Transforms the raw movie data to create the dimension table for spoken languages, and returns the dim_languages dataframe.
    """
    
    df["spoken_languages"] = df["spoken_languages"].apply(safe_eval)
    df = df.explode("spoken_languages")
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df .dropna(subset=["spoken_languages"]).reset_index(drop=True)
    df = pd.concat([df["id"], pd.json_normalize(df["spoken_languages"])], axis=1).rename(columns={"id": "movie_id", "iso_639_1": "language_code", "name": "language"}).dropna()
    df["movie_id"] = df["movie_id"].astype(int)   
   
    return df
def transform_fact_table(df1, df2):
    """
    Transforms and merges ratings and numerical movie columns into a fact table.
        - df1 (pd.DataFrame): The ratings dataframe from 'ratings_df'
        - df2 (pd.DataFrame): The main movie dataframe from 'movies_main_df'
    """

    df_summary = pd.json_normalize(df1["ratings_summary"])
    df1[df_summary.columns] = df_summary

    df1["last_rated"] = pd.to_datetime(df1["last_rated"], unit="s").dt.strftime("%m/%d/%Y")

    df1.drop(columns="ratings_summary", inplace=True)

    df2["id"] = pd.to_numeric(df2["id"], errors="coerce")
    df2["budget"] = pd.to_numeric(df2["budget"], errors="coerce")
    df2["revenue"] = pd.to_numeric(df2["revenue"], errors="coerce")

    fill_df1 = ["last_rated", "avg_rating", "total_ratings", "std_dev"]
    df1[fill_df1] = df1[fill_df1].fillna(0)
    df2[["budget", "revenue"]] = df2[["budget", "revenue"]].fillna(0)

    df2 = df2.dropna(subset=["id"]).drop(columns=["title", "release_date"]).rename(columns={"id": "movie_id"})
    df2["movie_id"] = df2["movie_id"].astype(int)

    df = pd.merge(df1, df2, on="movie_id", how="outer")

    df = df.drop_duplicates(subset=["movie_id"], keep="first")
    
    return df

# ===================
# === EXTRACT STEP ==
# ===================
# Extract data from zip file to Pandas DF
movies_main_df, movies_extended_df, ratings_df = extract_data(zip_url)

# ===================
# == TRANSFORM STEP =
# ===================
# Transform Pandas DF to star schema
DIM_movies_pd = transform_dim_movies(movies_main_df)
DIM_genres_pd = transform_dim_genres(movies_extended_df)
DIM_companies_pd = transform_dim_companies(movies_extended_df)
DIM_countries_pd = transform_dim_countries(movies_extended_df)
DIM_languages_pd = transform_dim_languages(movies_extended_df)
FACT_movies_pd = transform_fact_table(ratings_df, movies_main_df)
print("Transform done...")

# Convert Pandas to PySpark DF
DIM_movies = spark.createDataFrame(DIM_movies_pd)
DIM_genres = spark.createDataFrame(DIM_genres_pd)
DIM_companies = spark.createDataFrame(DIM_companies_pd)
DIM_countries = spark.createDataFrame(DIM_countries_pd)
DIM_languages = spark.createDataFrame(DIM_languages_pd)
FACT_movies = spark.createDataFrame(FACT_movies_pd)

# ============================
# ==== PYSPARK OPERATIONS ====
# ============================
print("Average Rating per Movie:")
FACT_movies.groupBy("movie_id").agg(
    {"avg_rating": "avg"}
).show()
FACT_movies = FACT_movies.withColumn(
    "rating_category",
    when(col("avg_rating") >= 4.5, "Very High")
    .when((col("avg_rating") >= 3.5) & (col("avg_rating") < 4.5), "High")
    .when((col("avg_rating") >= 2.5) & (col("avg_rating") < 3.5), "Average")
    .when((col("avg_rating") >= 1.5) & (col("avg_rating") < 2.5), "Low")
    .otherwise("Very Low")
)

print("Rating Categories:")
FACT_movies.select("movie_id", "avg_rating", "rating_category").show()

# Temp view for Spark SQL
FACT_movies.createOrReplaceTempView("fact_movies")
DIM_movies.createOrReplaceTempView("dim_movies")
DIM_genres.createOrReplaceTempView("dim_genres")
DIM_companies.createOrReplaceTempView("dim_companies")
DIM_countries.createOrReplaceTempView("dim_countries")
DIM_languages.createOrReplaceTempView("dim_languages")

print("Average Rating per Genre:")
spark.sql("""
    SELECT dg.genre, AVG(fm.avg_rating) AS avg_genre_rating
    FROM fact_movies fm
    JOIN dim_genres dg ON fm.movie_id = dg.movie_id
    JOIN dim_movies dm ON fm.movie_id = dm.movie_id
    GROUP BY dg.genre
""").show()

# ===================
# === LOAD STEP =====
# ===================
# Convert PySpark DF to Pandas for MySQL
DIM_movies_pd = DIM_movies.toPandas()
DIM_movies_pd = DIM_movies.toPandas()
DIM_genres_pd = DIM_genres.toPandas()
DIM_companies_pd = DIM_companies.toPandas()
DIM_countries_pd = DIM_countries.toPandas()
DIM_languages_pd = DIM_languages.toPandas()

# Create MySQL engine
dbuser = os.getenv("USER")
dbpassword = os.getenv("PASSWORD")
dbname = "finalprojectschema"
engine = create_engine(f"mysql+mysqlconnector://{dbuser}:{dbpassword}@localhost/{dbname}")

# Store DF to MySQL
DIM_movies_pd.to_sql(name="dim_movies", con=engine, index=False, if_exists="replace")
DIM_genres_pd.to_sql(name="dim_genres", con=engine, index=False, if_exists="replace")
DIM_companies_pd.to_sql(name="dim_companies", con=engine, index=False, if_exists="replace")
DIM_countries_pd.to_sql(name="dim_countries", con=engine, index=False, if_exists="replace")
DIM_languages_pd.to_sql(name="dim_languages", con=engine, index=False, if_exists="replace")
FACT_movies_pd.to_sql(name="fact_movies", con=engine, index=False, if_exists="replace")

print("Pipeline complete.")