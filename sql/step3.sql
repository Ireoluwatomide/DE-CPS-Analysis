SELECT
    a.household_identifier,
    TO_CHAR(a.interview_time, 'yyyy-MM') as interview_time,
    b.description as survey_outcome,
    c.description as housing_unit_type,
    d.description as household_type,
    e.telephone_in_house,
    f.access_to_telephone_elsewhere,
    g.telephone_interview_accepted,
    h.description as type_of_interview,
    j.description as faamily_income_range,
    k.description as geographic_division,
    l.description as race
FROM
    basic_monthly_cps.fct_dec_monthly_cps a
    LEFT JOIN
        basic_monthly_cps.dim_final_outcome b
    ON a.final_outcome = b.code
    LEFT JOIN
        basic_monthly_cps.dim_housing_unit_type c
    ON a.housing_unit = c.id
    LEFT JOIN
        basic_monthly_cps.dim_household_type d
    ON a.household_type = d.id
    LEFT JOIN
        basic_monthly_cps.dim_telephone_in_house e
    ON a.telephone_in_house = e.id
    LEFT JOIN
        basic_monthly_cps.dim_access_to_telephone_elsewhere f
    ON a.access_to_telephone_elsewhere = f.id
    LEFT JOIN
        basic_monthly_cps.dim_telephone_interview_accepted g
    ON a.telephone_interview_accepted = g.id
    LEFT JOIN
        basic_monthly_cps.dim_type_of_interview h
    ON a.type_of_interview = h.id
    LEFT JOIN
        basic_monthly_cps.dim_family_income j
    ON a.family_income_range = j.id
    LEFT JOIN
        basic_monthly_cps.dim_geographic_division k
    ON a.geographic_division = k.id
    LEFT JOIN
        basic_monthly_cps.dim_race l
    ON a.race = l.id

LIMIT 10
