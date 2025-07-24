SELECT
    date_utm.visit_date,
    date_utm.utm_source,
    date_utm.utm_medium,
    date_utm.utm_campaign,
    COUNT(DISTINCT s.visitor_id) AS visitors_count,
    COALESCE((
        SELECT SUM(daily_spent)
        FROM (
            SELECT utm_source, utm_medium, utm_campaign, campaign_date::date, daily_spent
            FROM public.ya_ads
            UNION ALL
            SELECT utm_source, utm_medium, utm_campaign, campaign_date::date, daily_spent
            FROM public.vk_ads
        ) ads
        WHERE ads.utm_source = date_utm.utm_source
        AND ads.utm_medium = date_utm.utm_medium
        AND ads.utm_campaign = date_utm.utm_campaign
        AND ads.campaign_date = date_utm.visit_date
    ), 0) AS total_cost,
    COUNT(DISTINCT l.lead_id) AS leads_count,
    COUNT(DISTINCT CASE 
                      WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
                      THEN l.lead_id 
                    END) AS purchases_count,
    SUM(CASE 
          WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
          THEN l.amount 
          ELSE 0 
        END) AS revenue
FROM (
    SELECT 
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.visitor_id,
        MAX(s.visit_date) OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date DESC
        ) AS last_click_date
    FROM public.sessions s
    JOIN public.leads l ON s.visitor_id = l.visitor_id
    WHERE s.visit_date <= l.created_at
    AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
) date_utm
JOIN public.sessions s ON 
    s.visit_date::date = date_utm.visit_date AND
    s.source = date_utm.utm_source AND
    s.medium = date_utm.utm_medium AND
    s.campaign = date_utm.utm_campaign
LEFT JOIN public.leads l ON 
    s.visitor_id = l.visitor_id AND
    s.visit_date <= l.created_at
WHERE s.visit_date = date_utm.last_click_date
GROUP BY 
    date_utm.visit_date,
    date_utm.utm_source,
    date_utm.utm_medium,
    date_utm.utm_campaign
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;