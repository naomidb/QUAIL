/*
name get

Gets a list of subjects and their consent dates for use with auditor
*/
SELECT s.dm_usubjid AS subject_id,
ic.consent_dssstdtc AS consent_date
FROM demographics AS s
JOIN informed_consent AS ic
ON ic.dm_subjid=s.dm_subjid
ORDER BY subject_id ;
