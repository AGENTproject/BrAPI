/*
BrAPI data type SAMPLE
materialised view to map LIMS data schema to SAMPLE data type 
*/

CREATE MATERIALIZED VIEW AGENT_READ.MV_BRAPI_SAMPLES
REFRESH 
NEXT SYSDATE + 1/24           
AS 
select
cast(null as varchar(4000)) as "additionalInfo",
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
infos.additionalinfo as "sampleDescription",
cast(null as varchar(255)) as "sampleGroupDbId",
material as "sampleName",
biosample_id as "samplePUI",
"sampleTimestamp" as "sampleTimestamp",
'DNA' as "sampleType",
cast(null as varchar(50)) as "studyDbId",
cast(null as varchar(255)) as "takenBy",
'leaf' as "tissueType",
cast(null as varchar(50)) as "trialDbId",
"well" as "well"
from
apexlimsophy.v_experimente_agent e join 
apexlimsophy.v_proben_agent p on p.r_v_experimente = e.id
join
(SELECT fremdid,biosample_id,"sampleBarcode","plateName","well","sampleTimestamp" FROM 
  ( 
    SELECT fremdid,parameter,wert 
    FROM apexlimsophy.v_ergebnisse_agent
  ) 
  PIVOT ( 
    MAX(wert) 
  FOR parameter in ('Barcode' "sampleBarcode",'extractplatebarcode' "plateName", 'extractplatewell' "well", 'Isolation' "sampleTimestamp", 'DB_ID' biosample_id))
) samp on p.fremdid = samp.fremdid
join (
select
distinct r_proben, listagg(parameter||': '||nvl(wert,'n/a'),'; ') additionalInfo from apexlimsophy.v_ergebnisse_agent where r_bereiche = 1098 group by r_proben
) infos on infos.r_proben=p.id
left outer join
(select min(r_proben) r_proben, fremdid
from
apexlimsophy.v_ergebnisse_agent 
where methode = 'Agent_Bonitur'
group by fremdid
) bonitur on bonitur.fremdid=samp.fremdid
where
anlage = 'NovaSeq_Sequenzierung' or anlage = 'IHAR-Sequenzierung'
;
