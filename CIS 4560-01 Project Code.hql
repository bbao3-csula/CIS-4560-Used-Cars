-- Upload compressed file to Linux File Systems
scp used_car_data.zip [your_user]@144.24.46.199:~​

-- Make UsedCars directory in HDFS
hdfs dfs –mkdir UsedCars

-- Unzip dataset in Linux File Systems
unzip used_car_data.zip

-- Put unzipped dataset into HDFS directory
hdfs dfs –put used_cars_data.csv UsedCars

-- Enter beeline client
beeline

-- Use personal database
use your-user;

-- Create the master table with all 66 columns
CREATE EXTERNAL TABLE used_cars_full (
    vin STRING,
    back_legroom FLOAT,
    bed STRING,
    bed_height FLOAT,
    bed_length FLOAT,
    body_type STRING,
    cabin STRING,
    city STRING,
    city_fuel_economy FLOAT,
    combine_fuel_economy FLOAT,
    daysonmarket INT,
    dealer_zip STRING,
    description STRING,
    engine_cylinders INT,
    engine_displacement FLOAT,
    engine_type STRING,
    exterior_color STRING,
    fleet BOOLEAN,
    frame_damaged BOOLEAN,
    franchise_dealer BOOLEAN,
    franchise_make STRING,
    front_legroom FLOAT,
    fuel_tank_volume FLOAT,
    fuel_type STRING,
    has_accidents BOOLEAN,
    height FLOAT,
    highway_fuel_economy FLOAT,
    horsepower INT,
    interior_color STRING,
    isCab BOOLEAN,
    is_certified BOOLEAN,
    is_cpo BOOLEAN,
    is_new BOOLEAN,
    is_oemcpo BOOLEAN,
    latitude FLOAT,
    length FLOAT,
    listed_date STRING,
    listing_color STRING,
    listing_id STRING,
    longitude FLOAT,
    main_picture_url STRING,
    major_options STRING,
    make_name STRING,
    maximum_seating INT,
    mileage INT,
    model_name STRING,
    owner_count INT,
    power FLOAT,
    price FLOAT,
    salvage BOOLEAN,
    savings_amount FLOAT,
    seller_rating FLOAT,
    sp_id STRING,
    sp_name STRING,
    theft_title BOOLEAN,
    torque STRING,
    transmission STRING,
    transmission_display STRING,
    trimId STRING,
    trim_name STRING,
    vehicle_damage_category STRING,
    wheel_system STRING,
    wheel_system_display STRING,
    wheelbase FLOAT,
    width FLOAT,
    year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/your-user/UsedCars';

-- Create simple table for more limited analysis
CREATE EXTERNAL TABLE used_cars_simple (
    vin STRING,
    make_name STRING,
    model_name STRING,
    year INT,
    price FLOAT,
    mileage INT,
    city STRING,
    fuel_type STRING,
    body_type STRING,
    transmission STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/your-user/UsedCars';

--Average City Fuel Economy by Body Type
SELECT body_type, ROUND(AVG(city_fuel_economy), 2) AS avg_mpg
FROM used_cars_full
WHERE city_fuel_economy IS NOT NULL AND body_type IS NOT NULL
GROUP BY body_type
ORDER BY avg_mpg DESC
LIMIT 27000;

--Average City Fuel Economy by Engine Cylinders
SELECT engine_cylinders, ROUND(AVG(city_fuel_economy), 2) AS avg_mpg
FROM used_cars_full
WHERE city_fuel_economy IS NOT NULL AND engine_cylinders IS NOT NULL
GROUP BY engine_cylinders
ORDER BY avg_mpg DESC
LIMIT 27000;

--Top 10 Franchise Makes
SELECT franchise_make, COUNT(*) AS count
FROM used_cars_full
WHERE franchise_make IS NOT NULL
GROUP BY franchise_make
ORDER BY count DESC
LIMIT 10;

-- Distribution of City Fuel Economy by Body Type and Vehicle Count
SELECT 
  body_type,
  COUNT(*) AS vehicle_count,
  ROUND(AVG(city_fuel_economy), 2) AS avg_city_mpg
FROM 
  used_cars_full
WHERE 
  body_type IS NOT NULL 
  AND city_fuel_economy IS NOT NULL
GROUP BY 
  body_type
ORDER BY 
  vehicle_count DESC;
LIMIT 10;

--Engine Displacement by Body Type
SELECT body_type, ROUND(AVG(engine_displacement), 2) AS avg_displacement
FROM used_cars_full
WHERE engine_displacement IS NOT NULL AND body_type IS NOT NULL
GROUP BY body_type
ORDER BY avg_displacement DESC
LIMIT 27000;

--Used Car Listings by Year
SELECT year, COUNT(*) AS num_listings
FROM used_cars_full
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year
LIMIT 27000;

-- Write simple table to HDFS
INSERT OVERWRITE DIRECTORY '/user/your-user/UsedCars'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    vin,
    make_name,
    model_name,
    year,
    price,
    mileage,
    city,
    fuel_type,
    body_type,
    transmission
FROM used_cars_full;

-- Merge files in HDFS
hdfs dfs -getmerge /user/your-user/UsedCars used_cars_simple.csv

--Check for file in Linux file system
$ ls

-- Download file to local machine
$ scp your_user@your_ip_address:/home/your_local_machine/used_cars_simple.csv ~