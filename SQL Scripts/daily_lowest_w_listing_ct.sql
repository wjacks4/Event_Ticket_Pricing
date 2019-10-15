drop table SEATGEEK_DAILY_LOWEST;

create table SEATGEEK_DAILY_LOWEST as 
select distinct * 
from (
	select B.*, C.listing_count 
	from (
		select distinct artist, name, venue, city, state, date_UTC, max(lowest_price) as lowest_price_day, create_date 
		from (
		  select *, date(create_ts) as create_date from seatgeek_events order by create_ts desc limit 1000000
		) 
		group by artist, name, venue, city, state, date_UTC, create_date
	) B join seatgeek_events C 
	on B.artist = C.artist and 
	B.name = C.name and 
	B.venue = C.venue and
	B.city = C.city and 
	B.state = C.state and 
	B.date_UTC = C.date_UTC and 
	B.lowest_price_day = C.lowest_price and 
	B.create_date = date(C.create_ts)
	order by artist, city, state, date_UTC, create_date
);


select * from SEATGEEK_DAILY_LOWEST;