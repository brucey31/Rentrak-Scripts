USE Reference_Tables;
ALTER TABLE CinemaListings ADD IF NOT EXISTS PARTITION (lasttouchdate = ${lasttouchdate}) LOCATION '${CinemaOutput}';
ALTER TABLE FilmListings ADD IF NOT EXISTS PARTITION (lasttouchdate = ${lasttouchdate}) LOCATION '${FilmOutput}';