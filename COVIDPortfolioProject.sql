
-- This COVID data is taken for 2 years period from 24 Feb 2020 to 8 Mar 2022. 
-- the dataset saved into 2 tables: death data and vaccination data

SELECT * 
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
ORDER BY 3,4

SELECT *
FROM COVIDPortfolioProject..COVIDvaccination
WHERE continent is not null
ORDER BY total_boosters

-- beware, some location are cummulative and grouped by certain filter. and continent is null and not null

-- first, see the bigger picture of the total cases vs deaths
-- and compared them between the first day and the last day retrieved

SELECT TOP 20 location, date, population, total_cases, new_cases, total_deaths, total_deaths_per_million 
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
ORDER BY 1,2

SELECT TOP 20 location, date, population, total_cases, new_cases, total_deaths, total_deaths_per_million 
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
ORDER BY 1 ASC,2 DESC

-- check the development of total cases vs population, showing the proportion of population got COVID
-- for example we only take Sweden, where I am studying rn, from their latest period

SELECT location, date, population, total_cases, ROUND(100*total_cases/population,2) as case_percentage
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE location = 'Sweden' 
ORDER BY 1,2 DESC

-- note: turns out the population data in this database is not dynamic :), shows the same between 2020 and 2022. 
-- check total cases percentage

SELECT location, population, MAX(total_cases) as total_cases_count, MAX(ROUND(100*total_cases/population,2)) 
	as case_percentage_pop
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
GROUP BY location, population
ORDER BY case_percentage_pop DESC

-- check the percentage development of death vs total case in Sweden, showing the probability of death from contracting COVID

SELECT location, date, total_cases, total_deaths, ROUND(100*total_deaths/total_cases,2) as death_percentage
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE location = 'Sweden'
ORDER BY 1,2 DESC

-- check total death percentage
-- total_deaths type is nvarchar --> cast to int

SELECT 
	location, 
	population, 
	MAX(total_cases) as total_cases_count, 
	MAX(ROUND(100*total_cases/population,2)) as case_percentage_pop, 
	MAX(cast(total_deaths as int)) as total_death_count, 
	MAX(ROUND(100*total_deaths/population,2)) as death_percentage_pop
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
GROUP BY location, population
ORDER BY total_death_count DESC

-- remove the cummulative location or other non-countries 
-- note aside of continent, they also categorize the country or location based on income level, e.g. low-income level
-- we need to remove that, by sorting where continent is not null, then apply to all commands

-- now we want to see the bigger picture, by looking by per continent

--** challenge with this dataset is that in continent column, the real continent doesnt show a correct number
-- and there is also continent in the location, which is more representative to other data source
-- which by March 2022 the total death in the world is around 6 million lives. 

-- [1] the first continent data based on location, where continent is NULL
SELECT 
	location,  
	MAX(total_cases) as total_cases_count, 
	MAX(ROUND(100*total_cases/population,2)) as case_percentage_pop, 
	MAX(cast(total_deaths as int)) as total_death_count, 
	MAX(ROUND(100*total_deaths/population,2)) as death_percentage_pop
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is null AND location NOT LIKE '%income%'
GROUP BY location
ORDER BY total_death_count DESC

-- the income group is problematic, need to remove that
-- Now we can see that the case percentage to population is highest in European Union (and Europe) 
-- but the death percentage to population is highest in South America
-- note, perhaps the 'International' is assumed as Antartica 

-- [2] based on continent column, where continent is not null 
-- run this and see the difference with the [1]

SELECT 
	continent,  
	MAX(total_cases) as total_cases_count, 
	MAX(ROUND(100*total_cases/population,2)) as case_percentage_pop, 
	MAX(cast(total_deaths as int)) as total_death_count, 
	MAX(ROUND(100*total_deaths/population,2)) as death_percentage_pop
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
GROUP BY continent
ORDER BY total_death_count DESC

-- checking aggregate globally per date

SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, ROUND(SUM(cast(new_deaths as int))/SUM(new_cases)*100,2)
	as death_percentage_perdate
FROM COVIDPortfolioProject..COVIDdeathdata
WHERE continent is not null
--GROUP BY date
ORDER BY 1,2

-- looking at the vaccination table
-- joining the two table (FK location & date) and using window function
-- how many people have get the vaccination to date, and the proportion to the population
-- also lets get the rolling cases, boosters and deaths in one table, so perhaps
-- we can gain more insights
-- unfortunately there is no data about daily new boosters :(

SELECT  
	deathdata.continent,
	deathdata.location, 
	deathdata.date, 
	deathdata.population, 
	deathdata.new_cases,
	SUM(deathdata.new_cases) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_cases,
	deathdata.new_deaths,
	SUM(CAST(deathdata.new_deaths as int)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_deaths,
	vaccinedata.new_vaccinations, 
	SUM(CAST(vaccinedata.new_vaccinations as bigint)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_vaccine,
	vaccinedata.total_boosters
FROM COVIDPortfolioProject..COVIDdeathdata deathdata
Join COVIDPortfolioProject..COVIDvaccination vaccinedata
	ON deathdata.location = vaccinedata.location
	and deathdata.date = vaccinedata.date
WHERE deathdata.continent is not null and deathdata.location = 'Australia' 
ORDER BY 3

-- just checking with above table, turns out the cummulative vaccine 
-- for Sweden is not updated by the country :(
-- also randomly checking, Australia, as of March 8, 2022, the cummulative vaccination is
-- 54,215,275 while their population is 25,788,217. So total vaccine is more than 2x
-- of the population. Also total boosters is 11,9 million. 
-- What we dont know here is that whether the total boosters already included in the 
-- vaccinations data. Logically it should be included, because standard of vaccination
-- is 2 times per person, more than 2 times is considered booster. And someone
-- cannot get booster if he/she hadnt get vaccinated. 


-- So, with above assumptions, lets substract the cummulative vaccinated 
-- with total_boosters :), to get true cummulative vaccine

-- create a temporary table using CTE

With JoinTableTemp (continent, location, date, population, new_cases, cummulative_cases, 
	new_deaths, cummulative_deaths, new_vaccinations, cummulative_vaccine, total_boosters) as 
(
SELECT  
	deathdata.continent,
	deathdata.location, 
	deathdata.date, 
	deathdata.population, 
	deathdata.new_cases,
	SUM(deathdata.new_cases) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_cases,
	deathdata.new_deaths,
	SUM(CAST(deathdata.new_deaths as int)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_deaths,
	vaccinedata.new_vaccinations, 
	SUM(CAST(vaccinedata.new_vaccinations as bigint)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_vaccine,
	CAST(vaccinedata.total_boosters as bigint)
FROM COVIDPortfolioProject..COVIDdeathdata deathdata
Join COVIDPortfolioProject..COVIDvaccination vaccinedata
	ON deathdata.location = vaccinedata.location
	and deathdata.date = vaccinedata.date
WHERE deathdata.continent is not null
)
SELECT *, ROUND(100*cummulative_vaccine/population,2) as percentage_vaccinated, 
	ROUND(100*total_boosters/population,2) as percentage_boosters, 
	(cummulative_vaccine-total_boosters) as cumm_true_vaccine,
	ROUND(100*(cummulative_vaccine-total_boosters)/population,2) as percentage_true_vaccine
FROM JoinTableTemp

-- lets create permanent table for above same function, 
-- but remove the cases and deaths, only vaccinated and boosters and true vaccinated.

DROP Table IF Exists PercentVaccine
CREATE Table PercentVaccine
( 
continent nvarchar(255),
location nvarchar(255), 
date datetime,
population numeric,
new_vaccinations numeric,
cummulative_vaccine numeric,
total_boosters numeric,
)
INSERT into PercentVaccine
SELECT  
	deathdata.continent,
	deathdata.location, 
	deathdata.date, 
	deathdata.population, 
	vaccinedata.new_vaccinations, 
	SUM(CAST(vaccinedata.new_vaccinations as bigint)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_vaccine,
	CAST(vaccinedata.total_boosters as bigint)
FROM COVIDPortfolioProject..COVIDdeathdata deathdata
Join COVIDPortfolioProject..COVIDvaccination vaccinedata
	ON deathdata.location = vaccinedata.location
	and deathdata.date = vaccinedata.date
WHERE deathdata.continent is not null

SELECT *, CAST(ROUND(100*cummulative_vaccine/population,2) as numeric(36,2)) as percentage_vaccinated, 
	CAST(ROUND(100*total_boosters/population,2) as numeric(36,2)) as percentage_boosters, 
	(cummulative_vaccine-total_boosters) as cumm_true_vaccine,
	CAST(ROUND(100*(cummulative_vaccine-total_boosters)/population,2) as numeric(36,2)) as percentage_true_vaccine
FROM PercentVaccine

-- note: since total booster is also not always filled in, when the total boosters is null,
-- then the the latest percentage_true_vaccine can be used as reference. 
-- if percent_true_vaccine is more than 100% then it means more than half population
-- already get vaccinated. the percentage_true_vaccine should not be more than >200%

-- Create VIEWS for data visualization 

CREATE View PercentVaccineView as
SELECT  
	deathdata.continent,
	deathdata.location, 
	deathdata.date, 
	deathdata.population, 
	vaccinedata.new_vaccinations, 
	SUM(CAST(vaccinedata.new_vaccinations as bigint)) 
		OVER (Partition by deathdata.location ORDER BY deathdata.location, 
		deathdata.date) as cummulative_vaccine,
	CAST(vaccinedata.total_boosters as bigint) as total_boosters
FROM COVIDPortfolioProject..COVIDdeathdata deathdata
Join COVIDPortfolioProject..COVIDvaccination vaccinedata
	ON deathdata.location = vaccinedata.location
	and deathdata.date = vaccinedata.date
WHERE deathdata.continent is not null

-- perhaps my note is too much in this project, thank you for reading. 
-- next probably I am gonna re-do and upload all Udacity project that I had done before 2 years ago :)
-- thanks to Alex the Analyst for giving the idea and guiding. 