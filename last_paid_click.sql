SELECT 
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM (
    -- Атрибутированные лиды (последний платный клик)
    SELECT 
        l.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM public.leads l
    JOIN (
        SELECT 
            visitor_id,
            visit_date,
            source,
            medium,
            campaign
        FROM (
            SELECT 
                visitor_id,
                visit_date,
                source,
                medium,
                campaign,
                ROW_NUMBER() OVER (PARTITION BY visitor_id ORDER BY visit_date DESC) AS rn
            FROM public.sessions
            WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        ) ranked_sessions
        WHERE rn = 1
    ) s ON l.visitor_id = s.visitor_id AND s.visit_date <= l.created_at
    
    UNION ALL
    
    -- Сессии без лидов
    SELECT 
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        NULL AS lead_id,
        NULL AS created_at,
        NULL AS amount,
        NULL AS closing_reason,
        NULL AS status_id
    FROM public.sessions
    WHERE LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    AND NOT EXISTS (
        SELECT 1 
        FROM public.leads 
        WHERE leads.visitor_id = sessions.visitor_id
    )
ORDER BY 
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;