CREATE KEYSPACE IF NOT EXISTS velib_db
       WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1};

use velib_db;

create table station (number int,
                     contract_name text,
                     name text,
                     address text,
                     position map<text,float>,
                     banking boolean,
                     bonus boolean,
                     status text,
                     bike_stands int,
                     available_bike_stands int,
                     available_bikes int,
                     last_update timestamp,
                     datetime timestamp,
                     primary key (number, last_update) );

create table station_static (number int,
                     contract_name text,
                     name text,
                     address text,
                     position map<text,float>,
                     banking boolean,
                     bonus boolean,
                     last_update timestamp,
                     primary key (number) );

create table station_dynamic (number int,
                     status text,
                     bike_stands int,
                     available_bike_stands int,
                     available_bikes int,
                     last_update timestamp,
                     datetime timestamp,
                     primary key (number, last_update));
