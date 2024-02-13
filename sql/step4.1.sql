SELECT
    b.description as family_income_range,
    COUNT(*) as responder_count
FROM
    basic_monthly_cps.fct_dec_monthly_cps a
LEFT JOIN 
    basic_monthly_cps.dim_family_income b
ON 
    a.family_income_range = b.id
GROUP BY
    b.description