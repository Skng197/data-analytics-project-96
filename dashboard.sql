--- метрики

--- CPU  Cost Per User Стоимость привлечения одного пользователя
SUM(total_cost) / NULLIF(SUM(visitors_count), 0);
---

--- ROI Return on Investment Окупаемость инвестиций
(COALESCE(SUM(revenue), 0) - COALESCE(SUM(total_cost), 0))
/ NULLIF(COALESCE(SUM(total_cost), 0), 0)
---

--- CPPU Cost Per Paying User Стоимость привлечения одного платящего пользователя
SUM(total_cost) / NULLIF(SUM(purchases_count), 0)
---

--- CPL Cost Per Lead Стоимость одного лида
SUM(total_cost) / NULLIF(SUM(visitors_count), 0)
---

--- CAC Customer Acquisition Cost Стоимость привлечения клиента
SUM(CASE WHEN "leads_count" > 0 THEN "total_cost" ELSE 0 END) 
/ 
NULLIF(SUM(CASE WHEN "leads_count" > 0 THEN "leads_count" ELSE 0 END), 0)
---

---Conversion Rate СК
SUM(purchases_count) / SUM(leads_count)
---

---Roi Percent
(SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100
---


