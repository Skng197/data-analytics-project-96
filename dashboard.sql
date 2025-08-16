WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source        AS utm_source,
        s.medium        AS utm_medium,
        s.campaign      AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions s
    LEFT JOIN leads l
        ON s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
    WHERE LOWER(s.medium) IN ('cpc','cpm','cpp','cpa','youtube','tg','social')
),
filtered_last_click AS (
    SELECT * FROM last_paid_click WHERE rn = 1
),
ad_costs AS (
    SELECT
        campaign_date::date      AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent)::numeric AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    ) ads
    GROUP BY 1,2,3,4
),
agg AS (
    SELECT
        f.visit_date::date AS visit_date,
        f.utm_source,
        f.utm_medium,
        f.utm_campaign,
        COUNT(*) AS visitors_count,
        COUNT(f.lead_id) AS leads_count,
        COUNT(*) FILTER (WHERE f.closing_reason = 'Успешно реализовано' OR f.status_id = 142) AS purchases_count,
        SUM(CASE WHEN f.closing_reason = 'Успешно реализовано' OR f.status_id = 142 THEN f.amount ELSE 0 END) AS revenue
    FROM filtered_last_click f
    GROUP BY 1,2,3,4
)
SELECT
    a.visit_date,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    a.visitors_count,
    COALESCE(c.total_cost, 0)::numeric AS total_cost,
    a.leads_count,
    a.purchases_count,
    a.revenue
FROM agg a
LEFT JOIN ad_costs c
  ON a.visit_date = c.visit_date
  AND a.utm_source = c.utm_source
  AND a.utm_medium = c.utm_medium
  AND a.utm_campaign = c.utm_campaign
ORDER BY
    a.revenue DESC NULLS LAST,
    a.visit_date ASC,
    a.visitors_count DESC,
    a.utm_source ASC,
    a.utm_medium ASC,
    a.utm_campaign ASC
    LIMIT 15
;
------------
WITH ads_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date::date AS date,
        SUM(daily_spent) AS cost
    FROM (
        SELECT utm_source, utm_medium, utm_campaign, campaign_date, daily_spent
        FROM vk_ads
        UNION ALL
        SELECT utm_source, utm_medium, utm_campaign, campaign_date, daily_spent
        FROM ya_ads
    ) t
    GROUP BY utm_source, utm_medium, utm_campaign, campaign_date::date
),
sessions_with_costs AS (
    SELECT
        s.visitor_id,
        s.visit_date::date AS date,
        s.source,
        s.medium,
        s.campaign,
        COALESCE(ac.cost, 0) AS cost
    FROM sessions s
    LEFT JOIN ads_costs ac
        ON s.source = ac.utm_source
        AND s.medium = ac.utm_medium
        AND s.campaign = ac.utm_campaign
        AND s.visit_date::date = ac.date
),
sessions_with_leads AS (
    SELECT
        swc.*,
        l.lead_id,
        l.amount
    FROM sessions_with_costs swc
    LEFT JOIN leads l
        ON swc.visitor_id = l.visitor_id
)
SELECT
    source AS channel,
    COUNT(DISTINCT lead_id) AS leads_count,
    COUNT(DISTINCT CASE WHEN amount > 0 THEN lead_id END) AS closed_leads,
    SUM(cost) AS total_cost,
    SUM(COALESCE(amount, 0)) AS total_revenue,
    ROUND((SUM(COALESCE(amount,0)) - SUM(cost)) / NULLIF(SUM(cost),0) * 100, 2) AS roi_percent,
    ROUND(SUM(cost) / NULLIF(COUNT(DISTINCT CASE WHEN amount > 0 THEN lead_id END),0), 2) AS cac,
    ROUND(COUNT(DISTINCT CASE WHEN amount > 0 THEN lead_id END)::NUMERIC / NULLIF(COUNT(DISTINCT lead_id),0) * 100, 2) AS cr_percent
FROM sessions_with_leads
GROUP BY source
ORDER BY roi_percent DESC;