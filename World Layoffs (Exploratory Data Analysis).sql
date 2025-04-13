-- Exploratory Data Analysis

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- this is telling me that the most amount of people laid off from a company was 12000 and that was 100% of the employees in the company

select *
from layoffs_staging2
where percentage_laid_off = 1;

-- the results show that all these company have laid off 100% of their employees

select *
from layoffs_staging2
order by funds_raised_millions desc;

-- the results show the amount of funds raised by these companies in millions of dollars

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- the results show that Britishvolt was the highest fund raiser with a 100% layoff

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- the results show the sum of total lay offs by company. we can see amazon, google and meta are among the top 10

select min(`date`), max(`date`)
from layoffs_staging2;

-- i want to see the date range of these lay offs and it seems like its almost exacvtly 3 years from march 2020 to march 2023
-- good to remember these are peak covid times

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- I can see that consumer, retail, transportation were all amongst the top 10 of most industry lay offs
-- this would make sense as during covid all of those industries financially regressed

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- The united states is leading the table with the most amount of layoffs by far

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

-- the most amount of lay offs happened in 2022, then 2023.alter
-- keep in mind this is only 3 months of data into 2023

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- most lay offs are coming from IPO's

select company, sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- percentage laid off is not very relevant because it is respective to each company

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

-- this is to see the total amount of lay offs per month

with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as ROLLING_TOTAL
from rolling_total;

-- the rolling total results show that:
-- 2021 was the best year in terms of lay offs
-- the end of 2022 and beginning of 2023 was the worst

select company, Year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company, Year(`date`)
order by company asc;

-- this is to see the total laid offs per year of each company

select company, Year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company, Year(`date`)
order by 3 desc;

-- the results show that google had the most lay offs in general in 2023
-- I want to see the top 5 total layoffs per year per company and give them a rank
-- dense rank was used because some are tied


with company_year (company, years, total_laid_off) as
(
select company, Year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company, Year(`date`)
), 
company_year_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not NULL
)
select*
from Company_Year_Rank
where Ranking <= 5
;





























