/* This is a toll_data set that we will analyse.
Here are some questions that helps us to dig into
what this data can tell us.
*/

-- First look at the data by selecting all columns
SELECT *
FROM toll_data_01;

/*
1. TEMPORAL ANALYSIS
*/
-- What are the peak hours for vehicle entries at toll plazas?
		
SELECT EXTRACT(HOUR FROM timestamp) AS hour, COUNT(*) AS entry_count	
FROM toll_data_01
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY entry_count DESC;

-- Vehicle entry frequency by day of the week

SELECT EXTRACT(DOW FROM timestamp) AS day_of_week, COUNT(*) AS entry_count
FROM toll_data_01
GROUP BY EXTRACT(DOW FROM timestamp)
ORDER BY day_of_week;

-- Seasonal trend in the data (monthly analysis)

SELECT EXTRACT(MONTH FROM timestamp) AS month, COUNT(*) AS entry_count
FROM toll_data_01
GROUP BY EXTRACT(MONTH FROM timestamp)
ORDER BY month;

/*
VEHICLE ANALYSIS
*/

-- Distribution of different vehicle types passing through the toll plazas

SELECT vehicle_type, COUNT(*) AS vehicle_count
FROM toll_data_01
GROUP BY vehicle_type
ORDER BY vehicle_count DESC;

-- Number of axles variation across different vehicle types

SELECT vehicle_type, number_of_axles, COUNT(*) AS count
FROM toll_data_01
GROUP BY vehicle_type, number_of_axles
ORDER BY vehicle_type, number_of_axles;

/*
PAYMENT ANALYSIS
*/

-- Most common types of payments used
SELECT type_of_payment, COUNT(*) AS payment_count
FROM toll_data_01
GROUP BY type_of_payment
ORDER BY payment_count DESC;

-- Correlation between type of payment and vehicle type or number of axles

SELECT type_of_payment, vehicle_type, COUNT(*) AS count
FROM toll_data_01
GROUP BY type_of_payment, vehicle_type
ORDER BY type_of_payment, vehicle_type;

SELECT type_of_payment, number_of_axles, COUNT(*) AS count
FROM toll_data_01
GROUP BY type_of_payment, number_of_axles
ORDER BY type_of_payment, number_of_axles;

/*
TOLL PLAZA ANALYSIS
*/

-- Toll plazas with the highest and lowest traffic volumes

SELECT tollplaza_id, COUNT(*) AS traffic_volume
FROM toll_data_01
GROUP BY tollplaza_id
ORDER BY traffic_volume DESC
-- LIMIT 10;

/*
VEHICLE CODE ANALYSIS
*/
