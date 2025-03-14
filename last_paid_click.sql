WITH paid_sessions AS (
    -- Выбираем только платные сессии
    SELECT
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM
        sessions
    WHERE
        medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
ranked_leads AS (
    -- Присваиваем ранг каждой сессии для каждого лидера (последняя платная сессия)
    SELECT
        l.visitor_id,
        l.lead_id,
        l.amount,
        l.created_at,
        l.closing_reason,
        l.status_id,
        ps.visit_date,
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.visitor_id, l.lead_id
            ORDER BY ps.visit_date DESC
        ) AS rn
    FROM
        leads l
    LEFT JOIN
        paid_sessions ps
    ON
        l.visitor_id = ps.visitor_id
        AND ps.visit_date <= l.created_at
),
attributed_leads AS (
    -- Оставляем только последнюю платную сессию для каждого лида
    SELECT
        visitor_id,
        lead_id,
        amount,
        created_at,
        closing_reason,
        status_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM
        ranked_leads
    WHERE
        rn = 1
),
all_visitors AS (
    -- Добавляем пользователей, которые не сконвертировались в лиды
    SELECT DISTINCT
        visitor_id,
        NULL::text AS lead_id,
        NULL::int AS amount,
        NULL::timestamp AS created_at,
        NULL::text AS closing_reason,
        NULL::bigint AS status_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM
        paid_sessions
    WHERE
        visitor_id NOT IN (SELECT DISTINCT visitor_id FROM leads)
)
-- Объединяем данные о лидерах и непреобразованных пользователях
SELECT
    COALESCE(al.visitor_id, av.visitor_id) AS visitor_id,
    COALESCE(al.visit_date, av.visit_date) AS visit_date,
    COALESCE(al.utm_source, av.utm_source) AS utm_source,
    COALESCE(al.utm_medium, av.utm_medium) AS utm_medium,
    COALESCE(al.utm_campaign, av.utm_campaign) AS utm_campaign,
    COALESCE(al.lead_id, av.lead_id) AS lead_id,
    COALESCE(al.created_at, av.created_at) AS created_at,
    COALESCE(al.amount, av.amount) AS amount,
    COALESCE(al.closing_reason, av.closing_reason) AS closing_reason,
    COALESCE(al.status_id, av.status_id) AS status_id
FROM
    attributed_leads al
FULL OUTER JOIN
    all_visitors av
ON
    al.visitor_id = av.visitor_id
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign asc
	limit 10;
