WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign
    FROM public.sessions s
    WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

ads_costs AS (
    SELECT
        campaign_date::date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM public.vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM public.ya_ads
    ) all_ads
    GROUP BY 1, 2, 3, 4
),

leads_lpc AS (
    SELECT
        l.visitor_id,
        s.visit_date::date AS visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.status_id,
        l.closing_reason,
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

filtered_leads AS (
    SELECT *
    FROM leads_lpc
    WHERE rn = 1
),

agg_data AS (
    SELECT
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(*) AS visitors_count,
        COUNT(DISTINCT fl.lead_id) AS leads_count,
        COUNT(DISTINCT CASE
            WHEN fl.closing_reason = 'Успешно реализовано' OR fl.status_id = 142
            THEN fl.lead_id END
        ) AS purchases_count,
        SUM(CASE
            WHEN fl.closing_reason = 'Успешно реализовано' OR fl.status_id = 142
            THEN fl.amount ELSE 0 END
        ) AS revenue
    FROM last_paid_click lpc
    LEFT JOIN filtered_leads fl
        ON lpc.visitor_id = fl.visitor_id
        AND lpc.visit_date = fl.visit_date
        AND lpc.utm_source = fl.utm_source
        AND lpc.utm_medium = fl.utm_medium
        AND lpc.utm_campaign = fl.utm_campaign
    GROUP BY lpc.visit_date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
),

final_result AS (
    SELECT
        a.visit_date,
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.visitors_count,
        COALESCE(ac.total_cost, 0) AS total_cost,
        a.leads_count,
        a.purchases_count,
        a.revenue
    FROM agg_data a
    LEFT JOIN ads_costs ac
        ON a.visit_date = ac.visit_date
        AND a.utm_source = ac.utm_source
        AND a.utm_medium = ac.utm_medium
        AND a.utm_campaign = ac.utm_campaign
)

SELECT *
FROM final_result
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;
