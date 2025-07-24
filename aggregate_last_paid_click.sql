WITH last_paid_clicks AS (
    SELECT
        s.visitor_id,
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
    FROM public.sessions s
    INNER JOIN public.leads l ON s.visitor_id = l.visitor_id
    WHERE s.visit_date <= l.created_at
    AND LOWER(COALESCE(s.medium, '')) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
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
    lpc.visit_date,
    COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    COALESCE(ac.daily_cost, 0) AS total_cost,
    COUNT(DISTINCT lpc.lead_id) AS leads_count,
    COUNT(DISTINCT CASE 
                      WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 
                      THEN lpc.lead_id 
                    END) AS purchases_count,
    COALESCE(SUM(CASE 
          WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 
          THEN lpc.amount 
          ELSE 0 
        END), 0) AS revenue
FROM last_paid_clicks lpc
LEFT JOIN ads_costs ac ON 
    lpc.visit_date = ac.cost_date AND
    lpc.utm_source = ac.utm_source AND
    lpc.utm_medium = ac.utm_medium AND
    lpc.utm_campaign = ac.utm_campaign
WHERE lpc.rn = 1
GROUP BY 
    lpc.visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    ac.daily_cost
ORDER BY
    revenue DESC NULLS LAST,
    lpc.visit_date ASC,
    visitors_count DESC,
    lpc.utm_source ASC,
    lpc.utm_medium ASC,
    lpc.utm_campaign ASC
LIMIT 15;