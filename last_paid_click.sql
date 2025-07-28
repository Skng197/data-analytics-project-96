WITH paid_sessions AS (
    SELECT
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM public.sessions
    WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

leads_with_ranked_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM paid_sessions s
    JOIN public.leads l
        ON l.visitor_id = s.visitor_id
        AND s.visit_date < l.created_at
),

last_paid_click AS (
    SELECT
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id
    FROM leads_with_ranked_sessions
    WHERE rn = 1
)

SELECT
    visitor_id,
    TO_CHAR(visit_date, 'YYYY-MM-DD HH24:MI:SS.MS') AS visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS.MS') AS created_at,
    amount,
    closing_reason,
    status_id
FROM last_paid_click
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
