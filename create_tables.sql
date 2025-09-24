-- ====================================================================
-- SQL Script for Creating the Flight Delays Star Schema
--
-- Project: Dimensional Modelling and ETL Data Processing
-- Author: Lascaro Gianluca, Morbidelli Filippo, Trincia Elio
--
-- Instructions:
-- 1. Create a new schema (database) named 'flight_delays_db'.
-- 2. Run this script to create all the necessary tables.
-- ====================================================================

-- Use the correct database
USE `flight_delays_db`;

-- Drop existing tables in reverse order of creation to avoid foreign key issues.
-- This allows the script to be run multiple times safely.
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS `f_flights`;
DROP TABLE IF EXISTS `d_time`;
DROP TABLE IF EXISTS `d_airports`;
DROP TABLE IF EXISTS `d_airlines`;
SET FOREIGN_KEY_CHECKS = 1;


-- ====================================================================
-- Dimension Tables Creation
-- ====================================================================

-- Dimension Table: d_airlines
-- Stores descriptive information about each airline.
CREATE TABLE `d_airlines` (
  `airline_id` INT NOT NULL AUTO_INCREMENT,
  `IATA_CODE` VARCHAR(3) NOT NULL,
  `AIRLINE` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`airline_id`),
  UNIQUE INDEX `iata_code_UNIQUE` (`IATA_CODE` ASC) VISIBLE
) ENGINE=InnoDB;


-- Dimension Table: d_airports
-- Stores descriptive information about each airport.
CREATE TABLE `d_airports` (
  `airport_id` INT NOT NULL AUTO_INCREMENT,
  `IATA_CODE` VARCHAR(3) NOT NULL,
  `AIRPORT` VARCHAR(255) NOT NULL,
  `CITY` VARCHAR(100),
  `STATE` VARCHAR(2),
  `COUNTRY` VARCHAR(3),
  `LATITUDE` DECIMAL(11, 8),
  `LONGITUDE` DECIMAL(11, 8),
  PRIMARY KEY (`airport_id`),
  UNIQUE INDEX `iata_code_UNIQUE` (`IATA_CODE` ASC) VISIBLE
) ENGINE=InnoDB;


-- Dimension Table: d_time
-- A calendar table to store date-related attributes for analysis.
CREATE TABLE `d_time` (
  `time_id` INT NOT NULL AUTO_INCREMENT,
  `date` DATE NOT NULL,
  `year` INT,
  `month` INT,
  `day` INT,
  `day_of_week` INT,
  `is_weekend` BOOLEAN,
  PRIMARY KEY (`time_id`),
  UNIQUE INDEX `date_UNIQUE` (`date` ASC) VISIBLE
) ENGINE=InnoDB;


-- ====================================================================
-- Fact Table Creation
-- ====================================================================

-- Fact Table: f_flights
-- Stores the numeric measures for each flight and links to the dimensions via foreign keys.
CREATE TABLE `f_flights` (
  `flight_id` INT NOT NULL AUTO_INCREMENT,
  `time_id` INT NOT NULL,
  `airline_id` INT NOT NULL,
  `origin_airport_id` INT NOT NULL,
  `destination_airport_id` INT NOT NULL,
  `DEPARTURE_DELAY` INT NULL,
  `ARRIVAL_DELAY` INT NULL,
  `AIR_TIME` FLOAT NULL,
  `DISTANCE` INT NULL,
  `CANCELLED` TINYINT NULL,
  `DIVERTED` TINYINT NULL,
  PRIMARY KEY (`flight_id`),
  INDEX `fk_time_idx` (`time_id` ASC) VISIBLE,
  INDEX `fk_airline_idx` (`airline_id` ASC) VISIBLE,
  INDEX `fk_origin_airport_idx` (`origin_airport_id` ASC) VISIBLE,
  INDEX `fk_destination_airport_idx` (`destination_airport_id` ASC) VISIBLE,
  CONSTRAINT `fk_flights_time`
    FOREIGN KEY (`time_id`)
    REFERENCES `d_time` (`time_id`),
  CONSTRAINT `fk_flights_airline`
    FOREIGN KEY (`airline_id`)
    REFERENCES `d_airlines` (`airline_id`),
  CONSTRAINT `fk_flights_origin_airport`
    FOREIGN KEY (`origin_airport_id`)
    REFERENCES `d_airports` (`airport_id`),
  CONSTRAINT `fk_flights_destination_airport`
    FOREIGN KEY (`destination_airport_id`)
    REFERENCES `d_airports` (`airport_id`)
) ENGINE=InnoDB;

-- ====================================================================
-- End of Script
-- ====================================================================