SELECT
    results.visit_date,
    results.utm_source,
    results.utm_medium,
    results.utm_campaign,
    results.visitors_count,
    results.total_cost,
    results.leads_count,
    results.purchases_count,
    results.revenue
FROM (
    SELECT
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(DISTINCT lpc.visitor_id) AS visitors_count,
        COALESCE((
            SELECT SUM(daily_spent) 
            FROM (
                SELECT utm_source, utm_medium, utm_campaign, campaign_date::date, daily_spent
                FROM public.ya_ads
                UNION ALL
                SELECT utm_source, utm_medium, utm_campaign, campaign_date::date, daily_spent
                FROM public.vk_ads
            ) combined_ads
            WHERE combined_ads.utm_source = lpc.utm_source
            AND combined_ads.utm_medium = lpc.utm_medium
            AND combined_ads.utm_campaign = lpc.utm_campaign
            AND combined_ads.campaign_date = lpc.visit_date
        ), 0) AS total_cost,
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
    GROUP BY 
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign
) results
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;