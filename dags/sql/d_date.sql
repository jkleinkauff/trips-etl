DROP TABLE IF EXISTS d_date CASCADE;
CREATE TABLE d_date (
    id INT GENERATED ALWAYS AS IDENTITY,
    date date,
    year int,
    month int,
    day int,
    PRIMARY KEY(id)
)