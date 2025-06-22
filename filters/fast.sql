-- PostgreSQL translation of the MongoDB pipeline from fast.json
-- Assumes a table named `alert` with a JSONB column `candidate`
-- Some parts (e.g., arrays) are modeled with LATERAL joins and CTEs

WITH base_filter AS (
    SELECT *
    FROM alert
    WHERE
        (candidate->>'rb')::float > 0.3 AND
        (candidate->>'drb')::float > 0.9 AND
        (candidate->>'magpsf')::float < 99 AND
        (candidate->>'ndethist')::int <= 200 AND
        (candidate->>'nbad')::int < 5 AND
        (candidate->>'fwhm')::float > 0.5 AND (candidate->>'fwhm')::float < 8 AND
        (candidate->>'isdiffpos')::boolean = TRUE AND
        ABS((candidate->>'jd')::float - (candidate->>'jdstarthist')::float) BETWEEN 0 AND 7.0 AND
        ABS((candidate->>'magpsf')::float - (candidate->>'magap')::float) < 0.75 AND
        (
            (candidate->>'sgscore1')::float <= 0.76 OR
            (candidate->>'distpsnr1')::float >= 2 OR
            (candidate->>'distpsnr1')::float < 0
        ) AND
        (
            (candidate->>'ssdistnr')::float < 0 OR
            (candidate->>'ssdistnr')::float >= 12 OR
            (candidate->>'ssmagnr')::float <= 0 OR
            (candidate->>'ssmagnr') IS NULL OR
            ABS((candidate->>'magpsf')::float - (candidate->>'ssmagnr')::float) >= 1
        ) AND
        (candidate->>'ncovhist')::int > (candidate->>'ndethist')::int
),

projected AS (
    SELECT
        objectId,
        (candidate->>'jd')::float AS t_now,
        (candidate->>'magpsf')::float AS m_now,
        (candidate->>'sigmapsf')::float AS merr_now,
        (candidate->>'band') AS band_now,
        prv_candidates,
        cross_matches,
        CASE
            WHEN (
                ((candidate->>'distpsnr1')::float < 20 AND (candidate->>'srmag1')::float BETWEEN 0 AND 15 AND (candidate->>'sgscore1')::float > 0.49) OR
                ((candidate->>'distpsnr2')::float < 20 AND (candidate->>'srmag2')::float BETWEEN 0 AND 15 AND (candidate->>'sgscore2')::float > 0.49) OR
                ((candidate->>'distpsnr3')::float < 20 AND (candidate->>'srmag3')::float BETWEEN 0 AND 15 AND (candidate->>'sgscore3')::float > 0.49) OR
                ((candidate->>'sgscore1')::float = 0.5 AND (candidate->>'distpsnr1')::float < 0.5 AND (
                    ABS((candidate->>'sgmag1')::float) < 17 OR ABS((candidate->>'srmag1')::float) < 17 OR ABS((candidate->>'simag1')::float) < 17
                ))
            ) THEN TRUE ELSE FALSE
        END AS brightstar,
        CASE
            WHEN (
                jsonb_array_length(cross_matches->'AllWISE') > 0 AND
                ((cross_matches->'AllWISE'->0->>'w1mpro')::float - (cross_matches->'AllWISE'->0->>'w2mpro')::float) > 0.8
            ) THEN TRUE ELSE FALSE
        END AS agn
    FROM base_filter
),

no_agn_or_bright AS (
    SELECT *
    FROM projected
    WHERE brightstar = FALSE AND agn = FALSE
),

-- Example to explode prv_candidates array
prv_exploded AS (
    SELECT
        n.objectId,
        n.t_now,
        n.m_now,
        n.merr_now,
        n.band_now,
        pc.elem->>'jd' AS jd,
        pc.elem->>'magpsf' AS magpsf,
        pc.elem->>'sigmapsf' AS sigmapsf,
        pc.elem->>'band' AS band,
        pc.elem->>'isdiffpos' AS isdiffpos
    FROM no_agn_or_bright n,
    LATERAL jsonb_array_elements(n.prv_candidates) AS pc(elem)
)

-- Further stages like detecting "stationary", "old", "fast_rising", "fast_fading"
-- would use window functions (e.g., LAG, LEAD) and time diff computations
-- You can define them in additional CTEs using the exploded prv_candidates
-- For example, calculate time/mag differences between adjacent detections
-- grouped by objectId and band, and detect slopes > 0.2 mag/day

-- Final SELECT depends on your annotation criteria:
-- WHERE stationary = true AND old = false AND count(prv_candidates) > 1
-- AND (fast_rising XOR fast_fading)

-- You can then project:
-- SELECT objectId, t_now AS annotations.passed_at, m_now AS annotations.current_magpsf
