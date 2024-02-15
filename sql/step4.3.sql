SELECT
    COUNT(a.household_identifier) AS responders_count
FROM
    basic_monthly_cps.fct_dec_monthly_cps a
WHERE
    telephone_in_house = 2
  AND access_to_telephone_elsewhere = 1
  AND telephone_interview_accepted = 1;