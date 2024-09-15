# Data cleaning

SELECT *
FROM layoffs;

# Step 1 - remove duplicates
# Step 2 - standardize the data
# Step 3 - look at null or blank values - see if we can populate
# Step 4 - remove any unneccesary columns (if you can)


# First, make a table that's a copy of the raw data 

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

# Now, only work off of the layoffs_staging table

# Step 1 - remove duplicates

# this will assign a row number to each unique combination of inputs in each row
-- be sure to partition by each row to ensure you catch real duplicates 
-- filter to only select row numbers over 1
-- if the row number is two or above, that means there is a duplicate

SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions, COUNT(*)
FROM layoffs_staging
GROUP BY 1, 2;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; 

# have to use ` instead of ' around date in list of rows

# create a new table to delete the duplicate rows
# right click on layoffs_staging table -- copy to clipboard -- create statement
# rename layoffs_staging2
# add row "row_num"

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# insert partitoned data with row num into layoffs_staging2

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# delete the duplicate rows and check

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

# note that deleting duplicates is a lot more simple if you have an "ID" column with unique identifiers

# Step 2 - standardize the data

# when we select all of the companies, we can see that 2 of them have a space at the front
-- use TRIM to remove the space 

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# check

SELECT company
FROM layoffs_staging2;

# look at the industries

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

# here we see we have Cyrpto, Crypto Currency, and CryptoCurrency.
-- need to update these to all be the same

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

# we see that most of them are "Crypto" so we want to change them all to be Crypto

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

# continue checking the different columns and scanning for inconsistencies and duplicates

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

# here we see that there is United States and United States. 

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

# OR do it like this

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

# change the date column from a text column, to a date column
-- below converts the current format (m/d/Y) to the standard date format

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;


# now change this from a text to a date column

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


# Step 3 - look at null or blank values - see if we can populate or remove

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
ANd percentage_laid_off IS NULL;

# if both the total laid off and % laid off are null, this is probably useless to us & can be deleted
-- come back to this

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# here we see entries where the industry is null or blank
-- see if the company comes up anywhere else with an industry that we can fill in

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# to see all of the entries where the company has at least one with the industry filled in,
# and at least one blank/null:

SELECT *
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

# change blank values to NULL

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

# update to get rid of NULL where possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

# in these entries, there's no information about the staff laid off, which is what we're looking for with this data set

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# we can delete these entries since they are missing essential info

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# now, we can get rid of the row_num column

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

#clean!

SELECT *
FROM layoffs_staging2;


