WITH cost_vk AS (
    SELECT 
        TO_CHAR(campaign_date, 'DD.MM.YYYY') AS campaign_day,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        SUM(daily_spent) AS vk_cost
    FROM public.vk_ads
    GROUP BY campaign_day, utm_source, utm_medium, utm_campaign, utm_content
),
cost_ya AS (
    SELECT 
        TO_CHAR(campaign_date, 'DD.MM.YYYY') AS campaign_day,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        SUM(daily_spent) AS ya_cost
    FROM public.ya_ads
    GROUP BY campaign_day, utm_source, utm_medium, utm_campaign, utm_content
),
cost_total AS (
    SELECT 
        COALESCE(v.campaign_day, y.campaign_day) AS campaign_day,
        COALESCE(v.utm_source, y.utm_source) AS utm_source,
        COALESCE(v.utm_medium, y.utm_medium) AS utm_medium,
        COALESCE(v.utm_campaign, y.utm_campaign) AS utm_campaign,
        COALESCE(v.utm_content, y.utm_content) AS utm_content,
        COALESCE(v.vk_cost, 0) + COALESCE(y.ya_cost, 0) AS total_cost
    FROM cost_vk v
    FULL OUTER JOIN cost_ya y
      ON v.campaign_day = y.campaign_day
     AND v.utm_source = y.utm_source
     AND v.utm_medium = y.utm_medium
     AND v.utm_campaign = y.utm_campaign
     AND v.utm_content = y.utm_content
),
leads_success AS (
    SELECT
        visitor_id,
        lead_id,
        amount,
        created_at,
        closing_reason,
        status_id
    FROM public.leads
    WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
)
SELECT
    TO_CHAR(s.visit_date, 'DD.MM.YYYY') AS visit_date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    s.campaign AS utm_campaign,
    s.content AS utm_content,
    COUNT(DISTINCT s.visitor_id) AS visitors_count,
    COALESCE(ct.total_cost, 0) AS total_cost,
    COUNT(DISTINCT l.lead_id) AS leads_count,
    COUNT(DISTINCT ls.lead_id) AS purchases_count,
    COALESCE(SUM(ls.amount), 0) AS revenue
FROM public.sessions s
LEFT JOIN public.leads l
    ON l.visitor_id = s.visitor_id
   AND s.visit_date < l.created_at
   AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
LEFT JOIN leads_success ls
    ON ls.visitor_id = s.visitor_id
   AND s.visit_date < ls.created_at
   AND LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
LEFT JOIN cost_total ct
    ON TO_CHAR(s.visit_date, 'DD.MM.YYYY') = ct.campaign_day
   AND s.source = ct.utm_source
   AND s.medium = ct.utm_medium
   AND s.campaign = ct.utm_campaign
   AND s.content = ct.utm_content
WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
GROUP BY
    s.visit_date, s.source, s.medium, s.campaign, s.content, ct.total_cost
ORDER BY
    revenue DESC NULLS LAST,
    s.visit_date ASC,
    visitors_count DESC,
    s.source ASC,
    s.medium ASC,
    s.campaign ASC,
    s.content ASC
LIMIT 15;