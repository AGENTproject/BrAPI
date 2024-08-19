/*
BrAPI data type SAMPLE
materialised view to map LIMS data schema to SAMPLE data type 
*/

CREATE MATERIALIZED VIEW AGENT_READ.MV_BRAPI_SAMPLES
REFRESH 
NEXT SYSDATE + 1/24           
AS 
select
additionalinfo as "additionalInfo",		
cast(null as varchar(50)) as "column",
samp.biosample_id as "externalReferences",
samp.fremdid as "germplasmDbId", 		
bonitur.r_proben as "observationUnitDbId",
cast(null as varchar(50)) as "plateDbId",
"plateName" as "plateName",
cast(null as varchar(255)) as "programDbId",
cast(null as varchar(50)) as "row",
"sampleBarcode" as "sampleBarcode",
p.id as "sampleDbId",
Probenbeschreibung as "sampleDescription",		
cast(null as varchar(255)) as "sampleGroupDbId",
material as "sampleName",
biosample_id as "samplePUI",
"sampleTimestamp" as "sampleTimestamp",
detaille_eng as "sampleType",		 
lab_name as "studyDbId", 		
objekte as "organism", 				
cast(null as varchar(255)) as "takenBy",
organe_eng as "tissueType",		
cast(null as varchar(50)) as "trialDbId",
"well" as "well"
from
apexlimsophy.v_experimente_agent e join 
apexlimsophy.v_proben_agent p on p.r_v_experimente = e.id
join
(SELECT fremdid,r_proben,biosample_id,"sampleBarcode","plateName","well","sampleTimestamp" FROM 
  ( 
    SELECT fremdid,r_proben,parameter,wert 
    FROM apexlimsophy.v_ergebnisse_agent
  ) 
  PIVOT ( 
    MAX(wert) 
  FOR parameter in ('Barcode' "sampleBarcode",'extractplatebarcode' "plateName", 'extractplatewell' "well", 'Isolation' "sampleTimestamp", 'DB_ID' biosample_id))
)samp on p.id = samp.r_proben
join (
select
distinct r_proben, listagg(parameter||': '||nvl(wert,'n/a'),'; ') AS additionalInfo from apexlimsophy.v_ergebnisse_agent where r_bereiche = 1098 group by r_proben
) infos on infos.r_proben=p.id
left outer join
(select min(r_proben) AS fremdid,r_proben
from
apexlimsophy.v_ergebnisse_agent 
where methode = 'Agent_Bonitur'
group by fremdid,r_proben
) bonitur on bonitur.fremdid=samp.fremdid AND  bonitur.r_proben = samp.r_proben
where R_probentyp in (1016 , 1027)