WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign
    FROM sessions AS s
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social'
        )
),

joined_data AS (
    SELECT
        ps.visitor_id,
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY ps.visit_date DESC
        ) AS rn
    FROM paid_sessions AS ps
    LEFT JOIN leads AS l
        ON
            ps.visitor_id = l.visitor_id
            AND ps.visit_date <= l.created_at
)

SELECT
    jd.visitor_id,
    jd.visit_date,
    jd.utm_source,
    jd.utm_medium,
    jd.utm_campaign,
    jd.lead_id,
    jd.created_at,
    jd.amount,
    jd.closing_reason,
    jd.status_id
FROM joined_data AS jd
WHERE jd.rn = 1 OR jd.lead_id IS NULL
ORDER BY
    jd.amount DESC NULLS LAST,
    jd.visit_date ASC,
    jd.utm_source ASC,
    jd.utm_medium ASC,
    jd.utm_campaign ASC
LIMIT 10;
