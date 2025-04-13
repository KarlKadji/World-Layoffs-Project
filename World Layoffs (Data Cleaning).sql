-- Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any columns


create table layoffs_staging -- I am creating an editable table to make sure I always have the raw data available just in case
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. Removing Duplicates
select *, 
row_number() over(
partition by company, 
industry, 
total_laid_off, 
percentage_laid_off, 
`date`) as row_num
from layoffs_staging;
-- using this partition, I assigned every row with a number that will help identify if it is a duplicate or not
-- a row number with 1 is not a duplicate, 2 is a duplicate

with duplicate_cte as 
(
select *, 
row_number() over(
partition by company, 
location,industry, 
total_laid_off, 
percentage_laid_off, 
`date`, 
stage,
country,
funds_raised_millions) as row_num
from layoffs_staging
)
-- I created a CTE to help me identify the duplicates in question. it was important to include every single column to make sure the matches would be identical
select * 
from duplicate_cte
where row_num > 1;
-- I now want to see all the duplicates
select *
from layoffs_staging
where company = 'Casper'
;

-- this is to double check that the results are accurate
-- to get the below query, right click on layoffs_staging, then copy to clipboard,create statement
-- I am creating another table to delete the duplicate rows, hence why it must be called layoffs_staging2
-- I added a row_num column and specified the values to be integers, this is so we can filter out the dup rows
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
;

-- This is to check that the columns appear, it is normal to not see any data at first since we must insert the data from the other table into this one

insert into layoffs_staging2
select *, 
row_number() over(
partition by company, 
location,industry, 
total_laid_off, 
percentage_laid_off, 
`date`, 
stage,
country,
funds_raised_millions) as row_num
from layoffs_staging;

-- this is inserting the data from layoffs_staging into layoffs_staging2 with no changes to it, they should be identical at this moment

select *
from layoffs_staging2
;

-- now we can see that all the data was inserted

select *
from layoffs_staging2
where row_num > 1;

-- this is to filter out the duplicates

delete
from layoffs_staging2
where row_num > 1;

-- this is to delete all the duplicates

select *
from layoffs_staging2
where row_num > 1;

-- now we see there are no duplicates

select *
from layoffs_staging2
;

-- now we have deleted all the duplicates from the table, row_num column will be deleted as it is a redundant column of only "1"

-- 2. Standardizing Data
-- finding issues in the data and fixing them

select company, (Trim(company))
from layoffs_staging2;
-- The TRIM function removes leading and trailing spaces (or other specified characters) from a string.

update layoffs_staging2
set company = TRIM(company);
-- I have now updated all the company names without any unnecessary spaces

select distinct industry
from layoffs_staging2
order by 1;

-- I filtered out all the disctinct industries and noticed that there are 3 of the same crypto industries in the result.

select *
from layoffs_staging2
where industry like 'Crypto%';
-- seeing that most results are listed under "Crypto" I will change them all to "Crypto" for consistency+

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';
-- I have updated all of the industry names for the crypto companies to "Crypto"

select distinct location
from layoffs_staging2
order by 1;
-- I have noticed that the locations with accents show up twice, one with no accent and another with symbols
-- I will update Dusseldorf, FLorianopolis, Malmo
-- I will refrain from using accents for simplicity

update layoffs_staging2 
set location = 'Dusseldorf'
where location like '%sseldorf';

update layoffs_staging2 
set location = 'Florianopolis'
where location like 'FlorianÃ³polis';

update layoffs_staging2 
set location = 'Malmo'
where location like 'Malm_';

select distinct country
from layoffs_staging2
order by 1;
-- I notice that there are two united states entries, this must be changed to "United States"
select distinct country, trim(trailing'.' from country)
from layoffs_staging2
Order by 1;
-- with the trim function i simply took off the period from the end of the country

update layoffs_staging2
set country =  trim(trailing'.' from country)
where country like 'United States%';
-- i updated the table to reflect the changes

-- we now want to change the dates from a text column to a date column

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging2;

-- we have updated the format now, however it is still a text column

alter table layoffs_staging2
modify column `date` date;

-- now the column has been changed to a date column. NOTE: do not do this on original dataset

select *
from layoffs_staging2;

-- 3. Null Values or Blank Values
update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null;

-- we first identify which industry values are empty or null

select *
from layoffs_staging2
where company = 'Airbnb';
-- I see that there was another entry where the data was inputted, so I will copy the industry

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb' and industry = null
;

-- this would be an option for just updating that one entry, but we want to do it to the whole table

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is NULL
and t2.industry is not null;
-- I am joining the table on the company to view the fields that require updating

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is NULL
and t2.industry is not null;
-- I am updating the fields that are NULL with those that are not

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where t1.industry is NULL
and t2.industry is not null;
-- now we see that there are no results, meaning there are no NULL spaces
select *
from layoffs_staging2
where industry is null;
-- to make sure, I ran this query again and saw that Bally's interactive is still null in the industry

select *
from layoffs_staging2
where company like 'Bally%';

-- the reason this row did not get update is because it is the only entry by this company
-- this is all the NULL we can replace as the other columns are based off data we cannot replicate or fill in unless we look online for the information

-- 4. Remove any columns/rows

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- since I cannot populate the null fields, these rows will be useless for the analysis
-- I will thus delete them
-- Note that these are deleted from our third generated table and not the original data

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- the rows have been deleted
-- I will now remove the row_num column

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;
-- the column has been removed
-- the data is now clean!





