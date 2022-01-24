DROP TABLE IF EXISTS f_trips_staging;
CREATE TABLE f_trips_staging (
    id INT GENERATED ALWAYS AS IDENTITY,
    origin_coord_x float,
    origin_coord_y float,
    origin_coord_point POINT GENERATED ALWAYS AS (point(origin_coord_x,origin_coord_y)) STORED,
    destination_coord_x float,
    destination_coord_y float,
    destination_coord_point POINT GENERATED ALWAYS AS (point(destination_coord_x,destination_coord_y)) STORED,
    date date,
    region VARCHAR(50),
    datasource VARCHAR(50),
    business_key UUID,
    PRIMARY KEY(id)
)