SELECT
    b.description as geographic_division,
    c.description as race,
    COUNT(*) as responder_count
FROM
    basic_monthly_cps.fct_dec_monthly_cps a
LEFT JOIN
    basic_monthly_cps.dim_geographic_division b
ON
    a.geographic_division = b.id
LEFT JOIN
    basic_monthly_cps.dim_race c
ON
    a.race = c.id
GROUP BY
    b.description,
    c.description
ORDER BY
    responder_count DESC

LIMIT 10