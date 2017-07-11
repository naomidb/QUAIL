/*
name run

Returns:
the subjects in redcap,
the subjects ended treatment,
the ones with a rna results form,
and finally the ones with an imported lab
when those labs take place after their end of treatment date

*/
CREATE TEMPORARY TABLE subjects_ended_treatment AS
SELECT e.eot_dsstdtc AS end_of_treatment_date,
date(e.eot_dsstdtc, '+252 days') AS date_range_end,
subject.dm_subjid,
subject.dm_usubjid AS subject_id,
batch.batch_name as batch_name
FROM early_discontinuation_eot AS e
JOIN demographics as subject
ON subject.dm_subjid=e.dm_subjid
JOIN batch
ON batch.batch_name=e.batch_name
WHERE LENGTH(eot_dsstdtc) > 1 ;

create temporary table hcv_abs as
select h.hcv_lbdtc as hcv_test_date,
s.subject_id as subject_id
from hcv_rna_results as h
join subjects_ended_treatment as s
on s.dm_subjid = h.dm_subjid
where hcv_test_date > s.end_of_treatment_date;


create temporary table hcv_im as
select h.hcv_im_lbdtc as hcv_test_date,
s.subject_id as subject_id
from hcv_rna_imported as h
join subjects_ended_treatment as s
on s.dm_subjid = h.dm_subjid
where hcv_test_date > s.end_of_treatment_date;

select (select
       count(distinct dm_subjid)
       from subject
       ) as number_subjects,
       (select
       count(distinct subject_id)
       from subjects_ended_treatment
       ) as number_subjects_ended_treatement,
       (select
       count(distinct hcv_abs.subject_id)
       from hcv_abs
       ) as number_with_hcv_rna_results_form_lab_after_eot,
       (select
       count(distinct hcv_im.subject_id)
       from hcv_im
       ) as number_with_hcv_imported_form_lab_after_eot;
