-- last_paid_click.sql
WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign
    FROM public.sessions AS s
    WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
last_paid_clicks AS (
    SELECT
        l.visitor_id,
        l.lead_id,
        l.amount,
        l.created_at,
        l.closing_reason,
        l.status_id,
        p.visit_date,
        p.utm_source,
        p.utm_medium,
        p.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY p.visit_date DESC
        ) AS rn
    FROM public.leads AS l
    LEFT JOIN paid_sessions AS p
        ON l.visitor_id = p.visitor_id
        AND p.visit_date <= l.created_at
)
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
FROM last_paid_clicks
WHERE rn = 1

UNION ALL

-- Пользователи без лидов
SELECT
    s.visitor_id,
    s.visit_date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    s.campaign AS utm_campaign,
    NULL::varchar AS lead_id,
    NULL::timestamp AS created_at,
    NULL::int4 AS amount,
    NULL::varchar AS closing_reason,
    NULL::int8 AS status_id
FROM public.sessions AS s
WHERE NOT EXISTS (
        SELECT 1
        FROM public.leads AS l
        WHERE l.visitor_id = s.visitor_id
    )
-- Сортировка
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC NULLS LAST,
    utm_medium ASC NULLS LAST,
    utm_campaign ASC NULLS LAST
LIMIT 10;