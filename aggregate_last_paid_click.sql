WITH last_paid_clicks AS (
    SELECT
        l.visitor_id,
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
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
        AND LOWER(COALESCE(s.medium, '')) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
filtered_last_paid_clicks AS (
    SELECT *
    FROM last_paid_clicks
    WHERE rn = 1
),
ads_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date::date AS cost_date,
        SUM(daily_spent) AS daily_cost
    FROM (
        SELECT 
            utm_source, utm_medium, utm_campaign, 
            campaign_date, daily_spent
        FROM public.ya_ads
        UNION ALL
        SELECT 
            utm_source, utm_medium, utm_campaign, 
            campaign_date, daily_spent
        FROM public.vk_ads
    ) combined_ads
    GROUP BY utm_source, utm_medium, utm_campaign, campaign_date::date
)
SELECT
    flpc.visit_date,
    COUNT(DISTINCT flpc.visitor_id) AS visitors_count,
    flpc.utm_source,
    flpc.utm_medium,
    flpc.utm_campaign,
    COALESCE(ac.daily_cost, 0) AS total_cost,
    COUNT(DISTINCT flpc.lead_id) AS leads_count,
    COUNT(DISTINCT CASE 
                      WHEN flpc.closing_reason = 'Успешно реализовано' OR flpc.status_id = 142 
                      THEN flpc.lead_id 
                    END) AS purchases_count,
    COALESCE(SUM(CASE 
          WHEN flpc.closing_reason = 'Успешно реализовано' OR flpc.status_id = 142 
          THEN flpc.amount 
          ELSE 0 
        END), 0) AS revenue
FROM filtered_last_paid_clicks flpc
LEFT JOIN ads_costs ac ON 
    flpc.visit_date = ac.cost_date AND
    flpc.utm_source = ac.utm_source AND
    flpc.utm_medium = ac.utm_medium AND
    flpc.utm_campaign = ac.utm_campaign
GROUP BY 
    flpc.visit_date,
    flpc.utm_source,
    flpc.utm_medium,
    flpc.utm_campaign,
    ac.daily_cost
ORDER BY
    revenue DESC NULLS LAST,
    flpc.visit_date ASC,
    flpc.utm_source ASC,
    flpc.utm_medium ASC,
    flpc.utm_campaign ASC
LIMIT 15;