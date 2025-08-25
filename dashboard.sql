-- last_paid_click

WITH joined_data AS (
    SELECT
        s.visitor_id,
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
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social'
        )
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
ORDER BY jd.amount DESC NULLS LAST, jd.visit_date ASC
LIMIT 10;

--- метрики

--- CPU Cost Per User Стоимость привлечения одного пользователя
SELECT SUM(total_cost) / NULLIF(SUM(visitors_count), 0);

--- ROI Return on Investment Окупаемость инвестиций
SELECT
    (COALESCE(SUM(revenue), 0) - COALESCE(SUM(total_cost), 0))
    / NULLIF(COALESCE(SUM(total_cost), 0), 0);

--- CPPU Cost Per Paying User Стоимость привлечения платящего пользователя
SELECT SUM(total_cost) / NULLIF(SUM(purchases_count), 0);

--- CPL Cost Per Lead Стоимость одного лида
SELECT SUM(total_cost) / NULLIF(SUM(visitors_count), 0);

--- CAC Customer Acquisition Cost Стоимость привлечения клиента
SELECT
    SUM(CASE WHEN leads_count > 0 THEN total_cost ELSE 0 END)
    / NULLIF(SUM(CASE WHEN leads_count > 0 THEN leads_count ELSE 0 END), 0);

--- Conversion Rate СК
SELECT SUM(purchases_count) / NULLIF(SUM(leads_count), 0);

--- Roi Percent
SELECT (SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100;
