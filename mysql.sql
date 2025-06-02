USE finalprojectschema;

-- DEFINING RELATIONSHIPS
ALTER TABLE fact_movies
ADD CONSTRAINT PK_FMovies PRIMARY KEY (movie_id);

ALTER TABLE dim_movies
ADD FOREIGN KEY (movie_id)
REFERENCES fact_movies(movie_id);

ALTER TABLE dim_companies
ADD FOREIGN KEY (movie_id)
REFERENCES fact_movies(movie_id);

ALTER TABLE dim_countries
ADD FOREIGN KEY (movie_id)
REFERENCES fact_movies(movie_id);

ALTER TABLE dim_genres
ADD FOREIGN KEY (movie_id)
REFERENCES fact_movies(movie_id);

ALTER TABLE dim_languages
ADD FOREIGN KEY (movie_id)
REFERENCES fact_movies(movie_id);

SELECT dg.genre, AVG(fm.avg_rating) AS avg_genre_rating
FROM fact_movies fm
JOIN dim_genres dg ON fm.movie_id = dg.movie_id
JOIN dim_movies dm ON fm.movie_id = dm.movie_id
GROUP BY dg.genre;

-- LAGLEAD
SELECT 
  title,
  avg_rating,
  RANK() OVER (ORDER BY avg_rating DESC) AS ranking,
  LAG(title) OVER (ORDER BY avg_rating DESC) AS prev_top_movie
FROM fact_movies
JOIN dim_movies USING (movie_id);

