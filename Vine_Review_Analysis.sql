/* DELIVERABLE 2 */

SELECT COUNT(*) FROM vine_table;
/* Filter the data and create a new DataFrame or table to retrieve all the rows 
where the total_votes count is equal to or greater than 20 to pick reviews that are 
more likely to be helpful and to avoid having division by zero errors later on. */

SELECT 	* 
INTO 	vine_TotalVotes_above20
FROM 	vine_table
WHERE 	total_votes >= 20
ORDER BY 4;

SELECT * FROM vine_TotalVotes_above20 LIMIT 10;
SELECT COUNT (*) FROM vine_TotalVotes_above20; --67,855

/* Filter the new DataFrame or table created in Step 1 and create a new DataFrame
or table to retrieve all the rows where the number of helpful_votes divided by 
total_votes is equal to or greater than 50%. */

SELECT 	*
INTO 	vine_above50percent
FROM 	vine_TotalVotes_above20
WHERE CAST(helpful_votes AS FLOAT)/CAST(total_votes AS FLOAT) >=0.5;

SELECT * FROM vine_above50percent LIMIT 10;
SELECT COUNT (*) FROM vine_above50percent; --61,948

/* Filter the DataFrame or table created in Step 2, and create a new 
DataFrame or table that retrieves all the rows where a review was 
written as part of the Vine program (paid), vine == 'Y'  */

--PAID
SELECT 	* 
INTO 	vine_yes_paid
FROM 	vine_above50percent
WHERE 	vine = 'Y';

SELECT * FROM vine_yes_paid LIMIT 10; 
SELECT COUNT (*) FROM vine_yes_paid; --334


/* Repeat Step 3, but this time retrieve all the rows where 
the review was not part of the Vine program (unpaid), vine == 'N'  */

--UNPAID
SELECT 	* 
INTO 	vine_no_unpaid
FROM 	vine_above50percent
WHERE 	vine = 'N';

SELECT * FROM vine_no_unpaid LIMIT 10;
SELECT COUNT (*) FROM vine_no_unpaid; --61614

/* Determine the total number of reviews,
the number of 5-star reviews, 
and the percentage of 5-star reviews for the two types of review (paid vs unpaid).*/

--PAID
SELECT COUNT(review_id) AS Total
FROM vine_yes_paid; --334

SELECT COUNT(review_id) AS FiveStar
FROM vine_yes_paid
WHERE star_rating = 5; --139


SELECT CAST(x.FiveStar AS FLOAT) / CAST(y.Total AS FLOAT) * 100
from
(
  (SELECT COUNT(review_id) AS FiveStar
   FROM vine_yes_paid WHERE star_rating = 5)
) x
join 
(
	SELECT COUNT(review_id) AS Total
	FROM vine_yes_paid
) y on 1=1

--======================================================================================
--UNPAID
SELECT COUNT(review_id) AS Total
FROM vine_no_unpaid; --61,614

SELECT COUNT(review_id) AS FiveStar
FROM vine_no_unpaid
WHERE star_rating = 5; --32,665


SELECT CAST(x.FiveStar AS FLOAT) / CAST(y.Total AS FLOAT) * 100
from
(
  (SELECT COUNT(review_id) AS FiveStar
   FROM vine_no_unpaid WHERE star_rating = 5)
) x
join 
(
	SELECT COUNT(review_id) AS Total
	FROM vine_no_unpaid
) y on 1=1
