-- Data Cleaning project 

-- 1. Remove duplicates
-- 2. Standardize the data (Strings)
-- 3. Null/Blank values
-- 4. Remove coluns



use World_layoffs;


SELECT 
    *
FROM
    layoffs;
    

create table layoffs_staging like layoffs;

Insert into layoffs_staging (
select * from layoffs

);


with  duplicate_cte as (
select * from (

SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
row_number() over(partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions )as row_num
from layoffs_staging 

)duplicates

where row_num>1


)
Delete from duplicate_cte;


-- creating a table and delete the rows.

drop table `layoffs_staging2`;

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


Insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete from `layoffs_staging2` 
where row_num>1;

-- STANDARDIZING DATA

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select * 
from `layoffs_staging2`
where industry like 'Crypto%';

select distinct industry 
from `layoffs`
order by 1;


select distinct country
from `layoffs_staging2`
order by 1;

update `layoffs_staging2`
set country = 'United States'
where country like 'United States_';

-- changing the date format and conversion

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

Alter table layoffs_staging2
Modify column `date` DATE;


-- Nulls and blanks handling

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select * 
from layoffs
where  industry = ' ';




select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location =t2.location
where t1.industry is null or t1.industry = ' '
 and t2.industry is not null;


DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select * from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

    
    