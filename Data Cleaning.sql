
-- SQL Project - Data Cleaning

--Data Source:  https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM layoffs;

--First thing we want to do is create a staging table. 
--This is the one we will work in and clean the data. We want a table with the raw data in case something happens
--CREATE TABLE layoffs_staging; 

SELECT *
INTO layoffs_staging
FROM layoffs;

--- Confirming the creation of the staging table
SELECT * 
FROM layoffs_staging;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

--First let's check for duplicates

-- i) Creating a row number 
SELECT company, industry, total_laid_off, "date",
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,"date" order by company ) AS row_num
	FROM 
		layoffs_staging ;

-- ii) looking for row numbers > 1

SELECT *
FROM (
	SELECT *, --company, industry, total_laid_off,"date",
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,"date",location,stage,country,funds_raised, percentage_laid_off
			ORDER BY company) AS row_num

	FROM layoffs_staging
) AS duplicates

	WHERE row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Oda'
;

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate
-- these are our real duplicates 
SELECT *
	FROM (
		SELECT company, "location", industry, total_laid_off,percentage_laid_off,"date", stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company, "location", industry, total_laid_off,percentage_laid_off,
			"date", stage, country, funds_raised
			Order by company) AS row_num
		FROM layoffs_staging
		) AS duplicates
WHERE row_num > 1;

-- Confirming the real duplicates 
SELECT *
FROM layoffs_staging
WHERE company = 'Beyond Meat'
;

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially
-- now you may want to write it like this: Using a CTE
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, "location", industry, total_laid_off,percentage_laid_off,"date", stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company, "location", industry, total_laid_off,percentage_laid_off,"date",
			stage, country, funds_raised
			ORDER BY company ) AS row_num
	FROM layoffs_staging
      ) duplicates WHERE row_num > 1
)

DELETE 
FROM DELETE_CTE ;


-- Checking if the duplicates have been deleted
SELECT *
FROM (
	SELECT company, "location", industry, total_laid_off,percentage_laid_off,"date", stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company,"location", industry, total_laid_off,percentage_laid_off,"date", stage, country, funds_raised
			ORDER BY company) AS row_num
	FROM layoffs_staging

) AS duplicates

WHERE row_num > 1;

--- duplicates are no longer there 

-- 2. Standardize Data

SELECT * 
FROM layoffs_staging ;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT (industry)
FROM layoffs_staging
ORDER BY industry;

SELECT company, TRIM(company)
FROM layoffs_staging
ORDER BY company;

UPDATE layoffs_staging
SET company = TRIM(company);
-- looking at the location to check for erorrs

SELECT DISTINCT ("location")
FROM layoffs_staging
ORDER BY "location";

SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoffs_staging
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoffs_staging
WHERE company LIKE 'airbnb%';



-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null
SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-----------------------------------------------------

--I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

SELECT *
FROM layoffs_staging
WHERE industry = 'crypto';

-- now that's taken care of:
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM layoffs_staging;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;


SELECT *
FROM layoffs_staging
WHERE country = 'Nigeria';


SELECT *
FROM layoffs_staging;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal.
--I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging;

-- A quick check that the useless data have been deleted
SELECT * 
FROM layoffs_staging
WHERE total_laid_off is null and percentage_laid_off is null;

































