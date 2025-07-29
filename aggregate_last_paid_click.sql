WITH last_paid_click AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions s
    LEFT JOIN leads l
        ON s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
    WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social')
),
filtered_last_click AS (
    SELECT *
    FROM last_paid_click
    WHERE rn = 1
),
ad_costs AS (
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
        FROM ya_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
    ) AS ads
    GROUP BY 1, 2, 3, 4
),
agg AS (
    SELECT
        f.visit_date::date AS visit_date,
        f.utm_source,
        f.utm_medium,
        f.utm_campaign,
        COUNT(*) AS visitors_count,
        COUNT(f.lead_id) AS leads_count,
        COUNT(*) FILTER (
            WHERE f.closing_reason = 'Успешно реализовано' OR f.status_id = 142
        ) AS purchases_count,
        SUM(CASE
            WHEN f.closing_reason = 'Успешно реализовано' OR f.status_id = 142
            THEN f.amount ELSE 0 END) AS revenue
    FROM filtered_last_click f
    GROUP BY 1, 2, 3, 4
),
final AS (
    SELECT
        a.visit_date,
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.visitors_count,
        COALESCE(c.total_cost, 0) AS total_cost,
        a.leads_count,
        a.purchases_count,
        a.revenue
    FROM agg a
    LEFT JOIN ad_costs c
        ON a.visit_date = c.visit_date
        AND a.utm_source = c.utm_source
        AND a.utm_medium = c.utm_medium
        AND a.utm_campaign = c.utm_campaign
)
SELECT *
FROM final
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;
