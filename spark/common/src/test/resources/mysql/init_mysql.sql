CREATE TABLE points
(
    name     VARCHAR(50),
    location GEOMETRY
);

INSERT INTO points (name, location)
VALUES ('Point A',
        ST_GeomFromText('POINT(10 20)', 4326));

INSERT INTO points (name, location)
VALUES ('Point B',
        ST_GeomFromText('POINT(30 40)', 4326));

INSERT INTO points (name, location)
VALUES ('Point C',
        ST_GeomFromText('POINT(50 60)', 4326));
