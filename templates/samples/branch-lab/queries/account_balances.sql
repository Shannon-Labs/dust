SELECT
    a.name AS account_name,
    sum(l.amount_cents) AS net_cents
FROM accounts a
JOIN ledger_entries l ON l.account_id = a.id
GROUP BY a.name
ORDER BY account_name ASC;
