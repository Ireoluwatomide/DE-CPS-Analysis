SELECT
    COUNT(*) AS responders_count
FROM
    basic_monthly_cps.fct_dec_monthly_cps
WHERE
    telephone_in_house = 1
  AND access_to_telephone_elsewhere = 1
  AND telephone_interview_accepted = 2;