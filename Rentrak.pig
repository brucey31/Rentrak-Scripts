register 'mongo-java-driver-2.12.2.jar';
register 'mongo-hadoop-core-1.4.0-ctm-20140714-SNAPSHOT.jar';
register 'mongo-hadoop-pig-1.4.0-ctm-20140714-SNAPSHOT.jar';
register 'piggybank.jar';




define IsoToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();



set DEFAULT_PARALLEL 50;

-- START OF PROCESSING FOR CINEMA FILE

RawDataCinema = LOAD 
	'/data_import/RentrakCinemaListings/Raw/Cinemas/*' 
using PigStorage(',') as (
TheatreCode:long,
TheatreName:chararray,
CircuitCode:int,
CircuitName:chararray,
StartDate:chararray,
EndDate:chararray,
FilmCode:long,
UKTitle:chararray,
ReleaseCode:long,
OriginalTitle:chararray,
DistributorCode:int,
DistributorName:chararray,
Country:chararray,
Genre:chararray);

ranked = rank RawDataCinema;

NoHeader = Filter ranked by (rank_RawDataCinema > 1);

Cleaner = foreach NoHeader generate 
IsoToUnix(ToString(CurrentTime(),'yyyy-MM-dd')) as Commitstamp,
TheatreCode as TheatreCode,
TheatreName as TheatreName,
CircuitCode as CircuitCode,
CircuitName as CircuitName,
CONCAT(CONCAT(SUBSTRING(StartDate,6,10),SUBSTRING(StartDate,3,5)),SUBSTRING(StartDate,0,2)) as StartDate,
CONCAT(CONCAT(SUBSTRING(EndDate,6,10),SUBSTRING(EndDate,3,5)),SUBSTRING(EndDate,0,2)) as EndDate,
FilmCode as FilmCode,
REPLACE(UKTitle, '"', '') as UKTitle,
ReleaseCode as ReleaseCode,
REPLACE(OriginalTitle, '"', '') as OriginalTitle,
DistributorCode as DistributorCode,
REPLACE(DistributorName, '"', '') as DistributorName,
Country as Country,
Genre as Genre;


--   START OF PROCESSING OF FILM FILE

RawDataFilm = LOAD 
	'/data_import/RentrakCinemaListings/Raw/Films/*' 
using PigStorage(',') as (
FilmCode:long,
OriginalTitle:chararray,
ReleaseCode:long,
UKTitle:chararray,
DistributorCode:long,
DistributorName:chararray,
ReleaseDate:chararray,
Territory:chararray);

ranked2 = rank RawDataFilm;

NoHeader2 = Filter ranked2 by (rank_RawDataFilm >1);

Cleaner2 =foreach NoHeader2 generate 
IsoToUnix(ToString(CurrentTime(),'yyyy-MM-dd')) as Commitstamp,
FilmCode as FilmCode,
REPLACE(OriginalTitle, '"', '') as OriginalTitle,
ReleaseCode as ReleaseCode,
REPLACE(UKTitle, '"', '') as UKTitle,
DistributorCode as DistributorCode,
REPLACE(DistributorName, '"', '') as DistributorName,
CONCAT(CONCAT(SUBSTRING(ReleaseDate,6,10),SUBSTRING(ReleaseDate,3,5)),SUBSTRING(ReleaseDate,0,2)) as ReleaseDate,
Territory as Territory;

/*

limiter = limit Cleaner2 50;

dump limiter;

*/




SET mapreduce.fileoutputcommitter.marksuccessfuljobs false;
   
store Cleaner into '${CinemaOutput}';   
   
store  Cleaner
into 'mongodb://Peg-ctmcmnddb02:27017/servetest.cinemalistings'
using com.mongodb.hadoop.pig.MongoInsertStorage();

store Cleaner2 into '${FilmOutput}';   
   
store  Cleaner2
into 'mongodb://Peg-ctmcmnddb02:27017/servetest.filmlistings'
using com.mongodb.hadoop.pig.MongoInsertStorage();


