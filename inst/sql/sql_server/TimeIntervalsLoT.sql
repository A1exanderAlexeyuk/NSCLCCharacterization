select *, abs(lag(regimen_end_date, 1) over (PARTITION BY person_id 
							   order by person_id) - regimen_start_date) as Treatment_free_Interval,
	CASE when abs(lead(regimen_start_date, 1) over (PARTITION BY person_id 
							   order by person_id) - regimen_start_date) IS NOT NULL
							   then abs(lead(regimen_start_date, 1) over (PARTITION BY person_id 
							   order by person_id) - regimen_start_date)
	when death_date - regimen_end_date >= 120 OR observation_end_date - regimen_start_date  >=120
				then coalesce(death_date, observation_end_date)	 - 	regimen_start_date
 	else coalesce(death_date, observation_end_date)	 - 	regimen_start_date  
		end as				
				Time_to_Treatment_Discontinuation
from tts group by 1,2,3,4,5,6,7 order by 1,5,2,3,4
