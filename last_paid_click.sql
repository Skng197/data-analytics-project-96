SELECT
    t.visitor_id,
    TO_CHAR(t.visit_date, 'DD.MM.YYYY') AS visit_date,
    t.utm_source,
    t.utm_medium,
    t.utm_campaign,
    t.lead_id,
    TO_CHAR(t.created_at, 'DD.MM.YYYY') AS created_at,
    t.amount,
    t.closing_reason,
    t.status_id
FROM (
    SELECT
        l.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.visitor_id  -- Исправлено с lead_id на visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM public.leads AS l
    JOIN public.sessions AS s
        ON l.visitor_id = s.visitor_id
        AND s.visit_date <= l.created_at  -- Добавлено = для включения одновременных визитов
        AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
) AS t
WHERE t.rn = 1
ORDER BY
    t.amount DESC NULLS LAST,
    t.visit_date ASC,
    t.utm_source ASC,
    t.utm_medium ASC,
    t.utm_campaign ASC
LIMIT 10;