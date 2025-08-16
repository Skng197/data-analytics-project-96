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
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpp', 'cpa', 'youtube', 'tg', 'social'
        )
),

filtered_last_click AS (
    SELECT
        lpc.visitor_id,
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        lpc.lead_id,
        lpc.created_at,
        lpc.amount,
        lpc.closing_reason,
        lpc.status_id,
        lpc.rn
    FROM last_paid_click AS lpc
    WHERE lpc.rn = 1
),

ad_costs AS (
    SELECT
        ads_agg.visit_date::date AS visit_date,
        ads_agg.utm_source,
        ads_agg.utm_medium,
        ads_agg.utm_campaign,
        SUM(ads_agg.daily_spent) AS total_cost
    FROM (
        SELECT
            ya.campaign_date AS visit_date,
            ya.utm_source,
            ya.utm_medium,
            ya.utm_campaign,
            ya.daily_spent
        FROM ya_ads AS ya
        UNION ALL
        SELECT
            vk.campaign_date AS visit_date,
            vk.utm_source,
            vk.utm_medium,
            vk.utm_campaign,
            vk.daily_spent
        FROM vk_ads AS vk
    ) AS ads_agg
    GROUP BY
        ads_agg.visit_date::date,
        ads_agg.utm_source,
        ads_agg.utm_medium,
        ads_agg.utm_campaign
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
                THEN f.amount
            ELSE 0
        END) AS revenue
    FROM filtered_last_click AS f
    GROUP BY
        f.visit_date::date,
        f.utm_source,
        f.utm_medium,
        f.utm_campaign
)

SELECT
    a.visit_date,
    a.visitors_count,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    a.leads_count,
    a.purchases_count,
    a.revenue,
    (c.total_cost)::numeric AS total_cost
FROM agg AS a
LEFT JOIN ad_costs AS c
    ON
        a.visit_date = c.visit_date
        AND a.utm_source = c.utm_source
        AND a.utm_medium = c.utm_medium
        AND a.utm_campaign = c.utm_campaign
ORDER BY
    a.revenue DESC NULLS LAST,
    a.visit_date ASC,
    a.visitors_count DESC,
    a.utm_source ASC,
    a.utm_medium ASC,
    a.utm_campaign ASC
LIMIT 15;
