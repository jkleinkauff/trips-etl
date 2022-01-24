DROP TABLE IF EXISTS f_trips;

CREATE TABLE f_trips (
    id INT GENERATED ALWAYS AS IDENTITY,
    origin point,
    destination point,
    sk_region INT,
    sk_datasource INT,
    sk_date INT,
    sk_business UUID,
    PRIMARY KEY(id),
    FOREIGN KEY(sk_region) REFERENCES d_region(id),
    FOREIGN KEY(sk_datasource) REFERENCES d_datasource(id),
    FOREIGN KEY(sk_date) REFERENCES d_date
)