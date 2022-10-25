Use cyclistic;

# For Trips Table
CREATE TABLE cyclistictrips (
trip_id INT,
starttime TEXT,
stoptime TEXT,
bikeid INT,
tripduration INT,
from_station_id INT,
from_station_name TEXT,
to_station_id INT,
to_station_name TEXT,
usertype TEXT,
gender TEXT,
birthyear TEXT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Divvy_Trips_2016_Q1.CSV' INTO TABLE cyclistictrips
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'      
IGNORE 1 ROWS
(trip_id, starttime, stoptime, bikeid, tripduration, from_station_id, from_station_name, to_station_id, to_station_name, usertype, gender, birthyear) 
       SET starttime = STR_TO_DATE(starttime, "%c/%d/%Y %H:%i:%s"),
           stoptime = STR_TO_DATE(stoptime, '%c/%d/%Y %H:%i:%s'),
           trip_id = IF(trip_id = '', NULL, trip_id), 
           starttime = IF(starttime = '', NULL, starttime),
           stoptime = IF(stoptime = '', NULL, stoptime),
           bikeid = IF(bikeid = '', NULL, bikeid),
           tripduration = IF(tripduration = '', NULL, tripduration), 
           from_station_id = IF(from_station_id = '', NULL, from_station_id), 
           from_station_name = IF(from_station_name = '', NULL, from_station_name), 
           to_station_id = IF(to_station_id = '', NULL, to_station_id),
		   to_station_name = IF(to_station_name = '', NULL, to_station_name), 
           usertype = IF(usertype = '', NULL, usertype), 
           gender = IF(gender = '', NULL, gender), 
           birthyear = IF(birthyear = '', NULL, birthyear);
ALTER TABLE `cyclistic`.`cyclistictrips` 
CHANGE COLUMN `starttime` `starttime` DATETIME NULL DEFAULT NULL ;
ALTER TABLE `cyclistic`.`cyclistictrips` 
CHANGE COLUMN `stoptime` `stoptime` DATETIME NULL DEFAULT NULL ;
ALTER TABLE `cyclistic`.`cyclistictrips` 
CHANGE COLUMN `birthyear` `birthyear` INT NULL DEFAULT NULL ;

# For Stations Table

CREATE TABLE cyclisticstations (
id INT,
name TEXT,
latitude DOUBLE,
longitude  DOUBLE,
dpcapacity INT,
online_date TEXT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Divvy_Stations_2016_Q4.CSV' INTO TABLE cyclisticstations
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'      
IGNORE 1 ROWS
(id, name, latitude, longitude, dpcapacity, online_date) 
       SET online_date = STR_TO_DATE(online_date, "%m/%d/%Y"),
           id = IF(id = '', NULL, id), 
           name = IF(name = '', NULL, name),
           latitude = IF(latitude = '', NULL, latitude),
           longitude = IF(longitude = '', NULL, longitude),
           dpcapacity = IF(dpcapacity = '', NULL, dpcapacity), 
           online_date = IF(online_date = '', NULL, online_date);
ALTER TABLE `cyclistic`.`cyclisticstations` 
CHANGE COLUMN `online_date` `online_date` DATE NULL DEFAULT NULL;

# Average Trip Length in Minutes by Usertypes
SELECT ROUND(AVG(tripduration)/60,2) AS 'Average Trip Length in Minutes',usertype AS 'Usertype' FROM cyclistictrips
GROUP BY usertype;

# Average Trip Length in Minutes by Usertypes based on Days
SELECT ROUND(AVG(tripduration)/60,2) AS 'Average Trip Length by Day in Minutes', DAYNAME(starttime) AS 'DAY', usertype AS 'Usertype' FROM cyclistictrips
GROUP BY DAYNAME(starttime), usertype
ORDER BY DAYOFWEEK(starttime),usertype;

# Number of Trips by Usertype based on Days in a Week
SELECT COUNT(DISTINCT(trip_id)) AS 'Number of Trips', DAYNAME(starttime) AS 'Day', usertype AS 'Usertype' FROM cyclistictrips
GROUP BY DAYNAME(starttime), usertype
ORDER BY DAYOFWEEK(starttime),usertype;

# Number of Trips by Usertypes Based on Months
SELECT COUNT(DISTINCT(trip_id)) AS 'Number_of_Trips', MONTHNAME(starttime) AS 'Month', usertype FROM cyclistictrips
GROUP BY MONTHNAME(starttime), usertype
ORDER BY MONTH(starttime),usertype;

# Trip Duration in Hours by Usertype
CREATE TABLE duration (
id INT AUTO_INCREMENT,
Time_Range TEXT,
Number_of_Customers INT,
Number_of_Subscribers INT,
PRIMARY KEY (id)
);
DELIMITER // #Procedure for inserting values in duration
CREATE procedure insert_values()
BEGIN
DECLARE x INT DEFAULT 1;
label_name: LOOP
  IF (x > 24) THEN
    LEAVE label_name;
  END IF;
	INSERT INTO duration(id) VALUES(x);
    SET x = x + 1;
END LOOP;
END //
DELIMITER ;
CALL insert_values();

DELIMITER // # Procedure for inputting time range with number of customers and subscribers 
CREATE procedure duration()
BEGIN
DECLARE time_duration_old INT DEFAULT 0;
DECLARE time_duration_new INT DEFAULT 3600;
DECLARE time_duration_in_hours_old INT DEFAULT 0;
DECLARE time_duration_in_hours_new INT DEFAULT 0;
DECLARE number_of_customer INT DEFAULT 0;
DECLARE number_of_subscriber INT DEFAULT 0;
DECLARE duration_id INT DEFAULT 1;
label_name: LOOP
  IF (time_duration_old > 86400) THEN
    LEAVE label_name;
  END IF;
  SET number_of_customer = (SELECT count(distinct(trip_id)) FROM cyclistictrips WHERE tripduration > time_duration_old AND tripduration < time_duration_new AND  usertype = 'Customer');
  SET number_of_subscriber = (SELECT count(distinct(trip_id)) FROM cyclistictrips WHERE tripduration > time_duration_old AND tripduration < time_duration_new AND usertype = 'Subscriber');
  SET time_duration_in_hours_old = time_duration_old / 3600;
  SET time_duration_in_hours_new = time_duration_new / 3600;
  UPDATE duration SET Number_of_Customers = number_of_customer WHERE id = duration_id;
  UPDATE duration SET Number_of_Subscribers = number_of_subscriber WHERE id = duration_id;
  UPDATE duration SET Time_Range = CONCAT(time_duration_in_hours_old," - ",time_duration_in_hours_new," Hours") WHERE id = duration_id;
  SET time_duration_old = time_duration_old + 3600;
  SET time_duration_new = time_duration_new + 3600;
  SET duration_id = duration_id + 1;
END LOOP;
END //
DELIMITER ;
CALL duration();

SELECT time_range, number_of_customers, number_of_subscribers FROM duration; # For excluding id values