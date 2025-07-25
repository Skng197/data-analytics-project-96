WITH last_paid_click AS (
    SELECT
        l.visitor_id,
        s.visit_date::date AS visit_date,
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
    FROM public.leads l
    JOIN public.sessions s
        ON l.visitor_id = s.visitor_id
        AND s.visit_date < l.created_at
        AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
attributed_leads AS (
    SELECT *
    FROM last_paid_click
    WHERE rn = 1
),
visits AS (
    SELECT
        visit_date::date AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        COUNT(*) AS visitors_count
    FROM public.sessions
    WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY 1, 2, 3, 4
),
costs AS (
    SELECT
        campaign_date::date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM public.vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM public.ya_ads
    ) ads
    GROUP BY 1, 2, 3, 4
),
leads_agg AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT lead_id) AS leads_count,
        COUNT(DISTINCT CASE
            WHEN LOWER(closing_reason) = 'успешно реализовано'
              OR status_id = 142 THEN lead_id
        END) AS purchases_count,
        SUM(CASE
            WHEN LOWER(closing_reason) = 'успешно реализовано'
              OR status_id = 142 THEN amount
        END) AS revenue
    FROM attributed_leads
    GROUP BY 1, 2, 3, 4
),
all_keys AS (
    SELECT DISTINCT visit_date, utm_source, utm_medium, utm_campaign FROM visits
    UNION
    SELECT DISTINCT visit_date, utm_source, utm_medium, utm_campaign FROM costs
    UNION
    SELECT DISTINCT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
)

SELECT
    k.visit_date,
    k.utm_source,
    k.utm_medium,
    k.utm_campaign,
    COALESCE(v.visitors_count, 0) AS visitors_count,
    COALESCE(c.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue, 0) AS revenue
FROM all_keys k
LEFT JOIN visits v
    ON k.visit_date = v.visit_date
    AND k.utm_source = v.utm_source
    AND k.utm_medium = v.utm_medium
    AND k.utm_campaign = v.utm_campaign
LEFT JOIN costs c
    ON k.visit_date = c.visit_date
    AND k.utm_source = c.utm_source
    AND k.utm_medium = c.utm_medium
    AND k.utm_campaign = c.utm_campaign
LEFT JOIN leads_agg l
    ON k.visit_date = l.visit_date
    AND k.utm_source = l.utm_source
    AND k.utm_medium = l.utm_medium
    AND k.utm_campaign = l.utm_campaign
ORDER BY
    revenue DESC NULLS LAST,
    k.visit_date ASC,
    visitors_count DESC,
    k.utm_source ASC,
    k.utm_medium ASC,
    k.utm_campaign ASC
LIMIT 15;
