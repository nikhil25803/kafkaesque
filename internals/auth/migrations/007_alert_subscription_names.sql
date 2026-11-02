ALTER TABLE alert_subscriptions ADD COLUMN name text;

UPDATE alert_subscriptions
SET name = left(
  consumer_group || ' / ' || topic || ' / ' ||
  CASE metric
    WHEN 'average' THEN 'Average lag'
    ELSE 'Partition lag'
  END,
  100
);

ALTER TABLE alert_subscriptions
ADD CONSTRAINT alert_subscriptions_name_check
CHECK (
  name IS NULL OR (
    name = btrim(name)
    AND char_length(name) BETWEEN 1 AND 100
  )
);
