SELECT 
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    v.total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue, 0) AS revenue
FROM (
    -- Агрегированные данные по визитам
    SELECT 
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count,
        SUM(daily_spent) AS total_cost
    FROM (
        -- Последние платные клики
        SELECT 
            visitor_id,
            visit_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM (
            -- Все платные сессии с ранжированием
            SELECT 
                s.visitor_id,
                s.visit_date::date AS visit_date,
                COALESCE(ya.utm_source, vk.utm_source, s.source) AS utm_source,
                COALESCE(ya.utm_medium, vk.utm_medium, s.medium) AS utm_medium,
                COALESCE(ya.utm_campaign, vk.utm_campaign, s.campaign) AS utm_campaign,
                COALESCE(ya.daily_spent, vk.daily_spent, 0) AS daily_spent,
                ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
            FROM 
                public.sessions s
            LEFT JOIN public.ya_ads ya 
                ON s.source = ya.utm_source 
                AND s.medium = ya.utm_medium 
                AND s.campaign = ya.utm_campaign
                AND s.visit_date::date = ya.campaign_date::date
            LEFT JOIN public.vk_ads vk 
                ON s.source = vk.utm_source 
                AND s.medium = vk.utm_medium 
                AND s.campaign = vk.utm_campaign
                AND s.visit_date::date = vk.campaign_date::date
            WHERE 
                LOWER(COALESCE(ya.utm_medium, vk.utm_medium, s.medium)) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        ) ranked_sessions
        WHERE rn = 1
    ) last_clicks
    GROUP BY 
        visit_date, utm_source, utm_medium, utm_campaign
) v
LEFT JOIN (
    -- Агрегированные данные по лидам
    SELECT 
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(l.lead_id) AS leads_count,
        COUNT(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.lead_id END) AS purchases_count,
        SUM(CASE WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 THEN l.amount ELSE 0 END) AS revenue
    FROM (
        -- Последние платные клики для лидов
        SELECT 
            visitor_id,
            visit_date,
            utm_source,
            utm_medium,
            utm_campaign
        FROM (
            SELECT 
                s.visitor_id,
                s.visit_date::date AS visit_date,
                COALESCE(ya.utm_source, vk.utm_source, s.source) AS utm_source,
                COALESCE(ya.utm_medium, vk.utm_medium, s.medium) AS utm_medium,
                COALESCE(ya.utm_campaign, vk.utm_campaign, s.campaign) AS utm_campaign,
                ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
            FROM 
                public.sessions s
            LEFT JOIN public.ya_ads ya 
                ON s.source = ya.utm_source 
                AND s.medium = ya.utm_medium 
                AND s.campaign = ya.utm_campaign
                AND s.visit_date::date = ya.campaign_date::date
            LEFT JOIN public.vk_ads vk 
                ON s.source = vk.utm_source 
                AND s.medium = vk.utm_medium 
                AND s.campaign = vk.utm_campaign
                AND s.visit_date::date = vk.campaign_date::date
            WHERE 
                LOWER(COALESCE(ya.utm_medium, vk.utm_medium, s.medium)) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        ) ranked_sessions
        WHERE rn = 1
    ) lpc
    LEFT JOIN 
        public.leads l ON lpc.visitor_id = l.visitor_id
    GROUP BY 
        lpc.visit_date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
) l ON v.visit_date = l.visit_date 
    AND v.utm_source = l.utm_source 
    AND v.utm_medium = l.utm_medium 
    AND v.utm_campaign = l.utm_campaign
ORDER BY 
    revenue DESC NULLS LAST,
    v.visit_date ASC,
    v.visitors_count DESC,
    v.utm_source ASC,
    v.utm_medium ASC,
    v.utm_campaign ASC
LIMIT 15;