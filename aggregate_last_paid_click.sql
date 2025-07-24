SELECT
    lpc.visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
    COALESCE(ac.daily_cost, 0) AS total_cost,
    COUNT(DISTINCT lpc.lead_id) AS leads_count,
    COUNT(DISTINCT CASE 
                      WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 
                      THEN lpc.lead_id 
                    END) AS purchases_count,
    SUM(CASE 
          WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 
          THEN lpc.amount 
          ELSE 0 
        END) AS revenue
FROM (
    SELECT
        s.visitor_id,
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM public.sessions s
    JOIN public.leads l ON s.visitor_id = l.visitor_id
    WHERE s.visit_date <= l.created_at
    AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND (s.visit_date, l.lead_id) IN (
        SELECT MAX(s2.visit_date), l2.lead_id
        FROM public.sessions s2
        JOIN public.leads l2 ON s2.visitor_id = l2.visitor_id
        WHERE s2.visit_date <= l2.created_at
        AND LOWER(s2.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        GROUP BY l2.lead_id
    )
) lpc
LEFT JOIN (
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
) ac ON 
    lpc.visit_date = ac.cost_date AND
    lpc.utm_source = ac.utm_source AND
    lpc.utm_medium = ac.utm_medium AND
    lpc.utm_campaign = ac.utm_campaign
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