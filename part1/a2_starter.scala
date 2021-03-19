//
// CS 245 Assignment 2, Part I starter code
//
// 1. Loads Cities and Countries as dataframes and creates views
//    so that we can issue SQL queries on them.
// 2. Runs 2 example queries, shows their results, and explains
//    their query plans.
//

// note that we use the default `spark` session provided by spark-shell
val cities = (spark.read
        .format("csv")
        .option("header", "true") // first line in file has headers
        .load("./Cities.csv"));
cities.createOrReplaceTempView("Cities")

val countries = (spark.read
        .format("csv")
        .option("header", "true")
        .load("./Countries.csv"));
countries.createOrReplaceTempView("Countries")

// look at the schemas for Cities and Countries
cities.printSchema()
countries.printSchema()

// Example 1
var df = spark.sql("SELECT city FROM Cities")
df.show()  // display the results of the SQL query
df.explain(true)  // explain the query plan in detail:
                  // parsed, analyzed, optimized, and physical plans

// Example 2
df = spark.sql("""
    SELECT *
    FROM Cities
    WHERE temp < 5 OR true
""")
df.show()
df.explain(true)



// Question 1
df = spark.sql("""
	SELECT country, EU
	FROM Countries
	WHERE coastline = "yes"
""")
df.show()
df.explain(true)


// Question 2

df = spark.sql("""
	SELECT city
	FROM (
		SELECT city, temp
		FROM cities
	)
	WHERE temp < 4
""")
df.show()
df.explain(true)


// Question 3
df = spark.sql("""
	SELECT *
	FROM Cities, Countries
	WHERE cities.country = Countries.country
		AND Cities.temp < 4
		AND Countries.pop > 6
""")
df.show()
df.explain(true)


// Question 4
df = spark.sql("""
	SELECT city, pop
	FROM Cities, Countries
	WHERE cities.country = Countries.country
		AND Countries.pop > 6
""")
df.show()
df.explain(true)


// Question 5
df = spark.sql("""
	SELECT *
	FROM Countries
	WHERE country LIKE "%e%d"
""")
df.show()
df.explain(true)


// Question 6
df = spark.sql("""
	SELECT *
	FROM Countries
	WHERE country LIKE "%ia"
""")
df.show()
df.explain(true)


// Question 7
df = spark.sql("""
	SELECT t1 + 1 as t2
	FROM (
		SELECT cast(temp as int) + 1 as t1
		FROM Cities
	)
""")
df.show()
df.explain(true)


// Question 8
df = spark.sql("""
	SELECT t1 + 1 as t2
	FROM (
		SELECT temp + 1 as t1
		FROM Cities
	)
""")
df.show()
df.explain(true)

