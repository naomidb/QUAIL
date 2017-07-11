/*
name run

Runs the Joy delta report
*/
CREATE TEMPORARY TABLE delta_subjects_ended_treatment AS
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

CREATE TEMPORARY TABLE delta_hcv AS
SELECT h.*,
s.*,
MIN(h.hcv_im_lbdtc) as first_hcv_followup
FROM hcv_rna_imported AS h
JOIN delta_subjects_ended_treatment AS s
ON s.dm_subjid=h.dm_subjid
WHERE s.end_of_treatment_date < h.hcv_im_lbdtc
GROUP BY s.subject_id ;

CREATE TEMPORARY TABLE delta_treatment_fields AS
SELECT subject.subject_id,
subject.end_of_treatment_date,
subject.date_range_end,
subject.dm_subjid,
c.cirr_suppfa_cirrstat AS cirrhosis_status,
d.dis_dsstdy AS duration_of_HCV_RNA_treatment,
t.reg_suppcm_regimen AS intended_treatment_regimen
FROM delta_subjects_ended_treatment as subject
JOIN cirrhosis AS c
ON subject.dm_subjid=c.dm_subjid
JOIN derived_values_baseline AS d
ON subject.dm_subjid=d.dm_subjid
JOIN treatment_regimen AS t
ON subject.dm_subjid=t.dm_subjid
ORDER BY subject_id, end_of_treatment_date ;


CREATE TEMPORARY TABLE delta_inr_imported_all AS
SELECT subject.subject_id,
subject.dm_subjid,
subject.end_of_treatment_date,
subject.date_range_end,
subject.cirrhosis_status,
subject.duration_of_HCV_RNA_treatment,
subject.intended_treatment_regimen,
i.inr_im_lbdtc AS test_date,
i.inr_im_lborres AS inr,
i.inr_imported_complete AS form_completed,
i.redcap_event_name AS unique_event_name,
i.batch_name
FROM inr_imported AS i
JOIN delta_treatment_fields as subject
ON subject.dm_subjid=i.dm_subjid
ORDER BY subject_id, end_of_treatment_date, test_date ;


CREATE TEMPORARY TABLE delta_inr_imported_in_range AS
SELECT * FROM delta_inr_imported_all
WHERE end_of_treatment_date < test_date
AND date_range_end >= test_date ;


CREATE TEMPORARY TABLE delta_inr_in_range_last AS
SELECT *, MAX(test_date) FROM delta_inr_imported_in_range AS range
GROUP BY subject_id;


CREATE TEMPORARY TABLE delta_inr_imported_baseline AS
SELECT *, MAX(test_date) AS test_date
FROM delta_inr_imported_all
WHERE end_of_treatment_date >= test_date
GROUP BY subject_id ;


CREATE TEMPORARY TABLE delta_inr AS
SELECT inr_all.* from delta_inr_imported_baseline as baseline
INNER JOIN delta_inr_imported_all as inr_all
ON baseline.test_date = inr_all.test_date
WHERE inr_all.subject_id = baseline.subject_id
UNION
SELECT * from delta_inr_imported_in_range;


CREATE TEMPORARY TABLE delta_chemistry_imported_all AS
SELECT subject.subject_id,
subject.end_of_treatment_date,
subject.date_range_end,
subject.cirrhosis_status,
subject.duration_of_HCV_RNA_treatment,
subject.intended_treatment_regimen,
c.chem_im_lbdtc AS test_date,
c.ast_im_lborres AS serum_ast,
c.alt_im_lborres AS serum_alt,
c.dbil_im_lborres AS direct_bilirubin,
c.tbil_im_lborres AS total_bilirubin,
c.chemistry_imported_complete AS form_completed,
c.redcap_event_name AS unique_event_name,
c.batch_name
FROM chemistry_imported AS c
JOIN delta_treatment_fields as subject
ON subject.dm_subjid=c.dm_subjid
ORDER BY subject_id, end_of_treatment_date, test_date ;


CREATE TEMPORARY TABLE delta_chemistry_imported_in_range AS
SELECT * FROM delta_chemistry_imported_all
WHERE end_of_treatment_date < test_date
AND date_range_end >= test_date ;


CREATE TEMPORARY TABLE delta_chemistry_in_range_last AS
SELECT *, MAX(test_date) FROM delta_chemistry_imported_in_range AS range
GROUP BY subject_id;


CREATE TEMPORARY TABLE delta_chemistry_imported_baseline AS
SELECT *, MAX(test_date) AS test_date
FROM delta_chemistry_imported_all
WHERE end_of_treatment_date >= test_date
GROUP BY subject_id ;


CREATE TEMPORARY TABLE delta_chem AS
SELECT chem_all.* from delta_chemistry_imported_baseline AS baseline
INNER JOIN delta_chemistry_imported_all AS chem_all
ON baseline.test_date = chem_all.test_date
WHERE baseline.subject_id = chem_all.subject_id
UNION
SELECT * from delta_chemistry_imported_in_range;


CREATE TEMPORARY TABLE delta_report AS
SELECT
base.subject_id,
base.cirrhosis_status,
base.duration_of_HCV_RNA_treatment,
base.intended_treatment_regimen,
latest.serum_ast - base.serum_ast AS serum_ast_delta,
latest.serum_alt - base.serum_alt AS serum_alt_delta,
latest.direct_bilirubin - base.direct_bilirubin AS direct_bilirubin_delta,
latest.total_bilirubin - base.total_bilirubin AS total_bilirubin_delta,
ilatest.INR - ibase.INR as INR_delta,
base.end_of_treatment_date,
base.test_date AS first_chem_followup,
latest.test_date AS last_chem_followup,
julianday(base.test_date) - julianday(base.end_of_treatment_date) AS days_from_eot_to_first_chem_followup,
julianday(latest.test_date) - julianday(base.end_of_treatment_date) AS days_from_eot_to_last_chem_followup,
ibase.test_date AS first_inr_followup,
ilatest.test_date AS last_inr_followup,
julianday(ibase.test_date) - julianday(ibase.end_of_treatment_date) AS days_from_eot_to_first_inr_followup,
julianday(ilatest.test_date) - julianday(ibase.end_of_treatment_date) AS days_from_eot_to_last_inr_followup,
hcv.first_hcv_followup,
julianday(hcv.first_hcv_followup) - julianday(base.end_of_treatment_date) as days_from_eot_to_first_hcv_followup
FROM delta_chemistry_imported_baseline AS base
JOIN delta_chemistry_in_range_last AS latest
ON base.subject_id = latest.subject_id
JOIN delta_inr_imported_baseline AS ibase
ON ibase.subject_id = base.subject_id
JOIN delta_inr_in_range_last AS ilatest
ON ilatest.subject_id = base.subject_id
JOIN delta_hcv as hcv
ON base.subject_id = hcv.subject_id
ORDER BY base.subject_id ;


SELECT * FROM delta_report;


